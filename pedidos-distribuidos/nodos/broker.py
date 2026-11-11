"""Nodo BROKER de mensajes (patron Mediator).

Ningun servicio conoce a los demas: todos hablan solo con el broker.
Ofrece dos mecanismos:
  * Cola de trabajo punto a punto (encolar / obtener / ack) para los pedidos.
  * Publicacion y suscripcion por temas (publicar / suscribir), base del
    patron Observer distribuido.

Cada conexion entrante se atiende en su propio hilo (modelo hilo por conexion).
Si un consumidor se desconecta con mensajes sin confirmar (ack), esos mensajes
vuelven al frente de la cola: asi no se pierden pedidos si un trabajador cae.
"""
import argparse
import itertools
import socket
import threading
import time
from collections import defaultdict, deque

import config
from comun.protocolo import Conexion
from comun.registro import Registro

log = Registro("BROKER")


class Broker:
    def __init__(self):
        self.colas = defaultdict(deque)
        self.cond = threading.Condition()
        self.suscriptores = defaultdict(list)
        self.cand_subs = threading.Lock()
        self.ids = itertools.count(1)
        self.pausado_hasta = 0.0
        self.stats = defaultdict(int)

    # ---- utilidades -------------------------------------------------------
    def _esperar_pausa(self):
        while time.time() < self.pausado_hasta:
            time.sleep(0.02)

    def _publicar(self, tema, mensaje):
        evento = {"op": "evento", "tema": tema, "msg": mensaje}
        with self.cand_subs:
            destinos = list(self.suscriptores[tema])
        for conn in destinos:
            try:
                conn.enviar(evento)
            except OSError:
                with self.cand_subs:
                    if conn in self.suscriptores[tema]:
                        self.suscriptores[tema].remove(conn)
        self.stats["eventos_publicados"] += 1

    # ---- atencion de una conexion (un hilo por conexion) -------------------
    def atender(self, conn, direccion):
        en_vuelo = {}
        temas = []
        try:
            while True:
                req = conn.recibir()
                op = req.get("op")
                if op == "pausar":  # simula saturacion o particion de red
                    self.pausado_hasta = time.time() + float(req["segundos"])
                    log.info(f"Broker PAUSADO {req['segundos']} s (simulacion de falla)")
                    conn.enviar({"ok": True})
                    continue
                self._esperar_pausa()

                if op == "encolar":
                    with self.cond:
                        self.colas[req["cola"]].append((next(self.ids), req["msg"]))
                        self.stats["encolados"] += 1
                        self.cond.notify()
                    conn.enviar({"ok": True})

                elif op == "obtener":
                    cola = self.colas[req["cola"]]
                    with self.cond:
                        self.cond.wait_for(lambda: len(cola) > 0, timeout=req.get("espera", 1.0))
                        item = cola.popleft() if cola else None
                    if item is None:
                        conn.enviar({"id": None})
                        continue
                    mid, msg = item
                    if req.get("ack"):
                        en_vuelo[mid] = (req["cola"], msg)
                    self.stats["entregados"] += 1
                    conn.enviar({"id": mid, "msg": msg})

                elif op == "ack":
                    en_vuelo.pop(req["id"], None)
                    self.stats["confirmados"] += 1

                elif op == "publicar":
                    self._publicar(req["tema"], req["msg"])

                elif op == "suscribir":
                    with self.cand_subs:
                        self.suscriptores[req["tema"]].append(conn)
                    temas.append(req["tema"])
                    conn.enviar({"ok": True})

                elif op == "stats":
                    with self.cond:
                        pendientes = {c: len(q) for c, q in self.colas.items()}
                    conn.enviar({"pendientes": pendientes, **self.stats})
        except (ConnectionError, OSError, ValueError):
            pass
        finally:
            if en_vuelo:
                with self.cond:
                    for mid in sorted(en_vuelo, reverse=True):
                        cola, msg = en_vuelo[mid]
                        self.colas[cola].appendleft((mid, msg))
                    self.stats["reentregados"] += len(en_vuelo)
                    self.cond.notify_all()
                log.info(f"Consumidor {direccion} desconectado: {len(en_vuelo)} pedido(s) sin ack devueltos a la cola")
            with self.cand_subs:
                for tema in temas:
                    if conn in self.suscriptores[tema]:
                        self.suscriptores[tema].remove(conn)
            conn.cerrar()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--puerto", type=int, default=config.PUERTO_BROKER)
    args = ap.parse_args()

    broker = Broker()
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((config.HOST, args.puerto))
    servidor.listen(256)
    log.info(f"Escuchando en {config.HOST}:{args.puerto}")
    while True:
        sock, direccion = servidor.accept()
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        threading.Thread(target=broker.atender, args=(Conexion(sock), direccion), daemon=True).start()


if __name__ == "__main__":
    main()
