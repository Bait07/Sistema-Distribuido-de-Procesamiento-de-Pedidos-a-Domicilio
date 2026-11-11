"""Nodo API GATEWAY (patron Proxy).

Es el unico punto de entrada para los clientes. Valida el pedido, lo deposita
en la cola del broker y responde de inmediato con 202 (procesamiento asincrono).
Ademas se suscribe al tema de estados (Observer) para conocer cuando termina
cada pedido y asi medir la latencia de punta a punta.

Version 1: abre una conexion TCP nueva al broker por cada pedido.
Version 2: usa un pool de conexiones persistentes protegido con Circuit Breaker
y desactiva el algoritmo de Nagle (TCP_NODELAY) en las respuestas HTTP.
"""
import argparse
import json
import queue
import socket
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import config
from comun.circuit_breaker import CircuitBreaker, CircuitoAbierto
from comun.protocolo import Conexion
from comun.registro import Registro

log = Registro("GATEWAY")
TIMEOUT_BROKER = 1.0


# ---- clientes del broker ---------------------------------------------------
class ClienteDirecto:
    """v1: una conexion nueva por peticion."""

    def __init__(self, puerto):
        self.puerto = puerto

    def encolar(self, pedido):
        conn = Conexion.conectar(config.HOST, self.puerto, timeout=TIMEOUT_BROKER)
        try:
            return conn.solicitar({"op": "encolar", "cola": config.COLA_PEDIDOS, "msg": pedido})
        finally:
            conn.cerrar()


class ClientePool:
    """v2: pool de conexiones reutilizables (evita el costo de conectar cada vez)."""

    def __init__(self, puerto, tamano=32):
        self.puerto = puerto
        self.libres = queue.LifoQueue(maxsize=tamano)

    def encolar(self, pedido):
        try:
            conn = self.libres.get_nowait()
        except queue.Empty:
            conn = Conexion.conectar(config.HOST, self.puerto, timeout=TIMEOUT_BROKER)
        try:
            resp = conn.solicitar({"op": "encolar", "cola": config.COLA_PEDIDOS, "msg": pedido})
        except Exception:
            conn.cerrar()
            raise
        try:
            self.libres.put_nowait(conn)
        except queue.Full:
            conn.cerrar()
        return resp


# ---- estado del gateway ------------------------------------------------------
class Estado:
    def __init__(self):
        self.cand = threading.Lock()
        self.pedidos = {}
        self.aceptados = 0
        self.rechazados = 0
        self.fallidos = 0
        self.completados = 0
        self.latencias = []
        self.primer_aceptado = None
        self.ultimo_completado = None

    def metricas(self, circuito):
        with self.cand:
            lat = sorted(self.latencias)
            def pct(p):
                return round(lat[min(len(lat) - 1, int(p * len(lat)))] * 1000, 1) if lat else None
            return {
                "aceptados": self.aceptados,
                "rechazados": self.rechazados,
                "fallidos": self.fallidos,
                "completados": self.completados,
                "latencia_ms": {
                    "p50": pct(0.50), "p95": pct(0.95), "max": pct(1.0),
                    "promedio": round(sum(lat) / len(lat) * 1000, 1) if lat else None,
                },
                "primer_aceptado": self.primer_aceptado,
                "ultimo_completado": self.ultimo_completado,
                "circuito": circuito,
            }


def validar(pedido):
    if not isinstance(pedido, dict):
        return "el cuerpo debe ser un objeto JSON"
    if not str(pedido.get("cliente", "")).strip():
        return "falta el cliente"
    items = pedido.get("items")
    if not isinstance(items, list) or not items:
        return "el pedido debe tener al menos un producto"
    for it in items:
        if not isinstance(it, dict) or not it.get("producto") or int(it.get("cantidad", 0)) <= 0:
            return "producto o cantidad invalida"
    if float(pedido.get("total", 0)) <= 0:
        return "el total debe ser mayor que cero"
    return None


def escuchar_estados(puerto_broker, estado):
    """Observer: recibe los cambios de estado publicados por los trabajadores."""
    while True:
        try:
            conn = Conexion.conectar(config.HOST, puerto_broker)
            conn.solicitar({"op": "suscribir", "tema": config.TEMA_ESTADO})
            while True:
                ev = conn.recibir()["msg"]
                with estado.cand:
                    p = estado.pedidos.get(ev["id"])
                    if p is None or p["estado"] == "completado":
                        continue
                    p["estado"] = ev["estado"]
                    p["trabajador"] = ev.get("trabajador")
                    if ev["estado"] == "completado":
                        p["completado"] = ev["ts"]
                        estado.completados += 1
                        estado.latencias.append(ev["ts"] - p["creado"])
                        estado.ultimo_completado = ev["ts"]
        except (ConnectionError, OSError, ValueError):
            time.sleep(0.2)


class Servidor(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 512  # cola de conexiones pendientes (backlog) amplia


def crear_manejador(cliente, breaker, estado, nodelay):
    class Manejador(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            if nodelay:  # v2: desactiva Nagle para no esperar el ACK retardado del cliente
                self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        def log_message(self, *args):
            pass

        def _responder(self, codigo, cuerpo):
            datos = json.dumps(cuerpo, ensure_ascii=False).encode("utf-8")
            self.send_response(codigo)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(datos)))
            self.end_headers()
            self.wfile.write(datos)

        def do_POST(self):
            if self.path != "/pedidos":
                return self._responder(404, {"error": "ruta no encontrada"})
            largo = int(self.headers.get("Content-Length", 0))
            try:
                pedido = json.loads(self.rfile.read(largo) or b"{}")
                error = validar(pedido)
            except (ValueError, TypeError):
                error = "JSON invalido"
            if error:
                with estado.cand:
                    estado.rechazados += 1
                return self._responder(400, {"error": error})

            pedido["id"] = uuid.uuid4().hex[:10]
            creado = time.time()
            with estado.cand:  # se registra antes de encolar para no perder eventos tempranos
                estado.pedidos[pedido["id"]] = {"estado": "recibido", "creado": creado}
            try:
                if breaker:
                    breaker.llamar(cliente.encolar, pedido)
                else:
                    cliente.encolar(pedido)
            except (CircuitoAbierto, OSError, ConnectionError, TimeoutError) as e:
                with estado.cand:
                    estado.pedidos.pop(pedido["id"], None)
                    estado.fallidos += 1
                return self._responder(503, {"error": "servicio de pedidos no disponible", "detalle": type(e).__name__})

            with estado.cand:
                estado.aceptados += 1
                if estado.primer_aceptado is None:
                    estado.primer_aceptado = creado
            self._responder(202, {"id": pedido["id"], "estado": "recibido"})

        def do_GET(self):
            if self.path == "/metricas":
                return self._responder(200, estado.metricas(breaker.estado if breaker else "SIN_CB"))
            if self.path.startswith("/pedidos/"):
                pid = self.path.rsplit("/", 1)[-1]
                with estado.cand:
                    p = estado.pedidos.get(pid)
                    p = dict(p) if p else None
                return self._responder(200 if p else 404, p or {"error": "pedido no encontrado"})
            self._responder(404, {"error": "ruta no encontrada"})

    return Manejador


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--puerto", type=int, default=config.PUERTO_GATEWAY)
    ap.add_argument("--puerto-broker", type=int, default=config.PUERTO_BROKER)
    ap.add_argument("--version", choices=["v1", "v2"], default="v2")
    args = ap.parse_args()
    perfil = config.VERSIONES[args.version]

    cliente = ClientePool(args.puerto_broker) if perfil["pool"] else ClienteDirecto(args.puerto_broker)
    breaker = CircuitBreaker(3, 2.0, log) if perfil["circuit_breaker"] else None
    estado = Estado()
    threading.Thread(target=escuchar_estados, args=(args.puerto_broker, estado), daemon=True).start()

    servidor = Servidor((config.HOST, args.puerto), crear_manejador(cliente, breaker, estado, perfil["nodelay"]))
    log.info(f"API Gateway {args.version} en http://{config.HOST}:{args.puerto}  (pool={perfil['pool']}, circuit breaker={perfil['circuit_breaker']}, nodelay={perfil['nodelay']})")
    servidor.serve_forever()


if __name__ == "__main__":
    main()
