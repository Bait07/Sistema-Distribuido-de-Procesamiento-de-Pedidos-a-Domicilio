"""Nodo TRABAJADOR (patron Productor Consumidor con pool de hilos).

Cada trabajador ejecuta `hilos` hilos consumidores. Cada hilo tiene su propia
conexion al broker, toma un pedido, lo procesa en tres etapas (validar
inventario, cobrar, asignar repartidor) y publica los cambios de estado.

Con ack activo (v2) el pedido solo se confirma al terminar; si el proceso
muere a mitad de camino el broker lo devuelve a la cola.
Un hilo adicional envia latidos (heartbeat) al monitor.
"""
import argparse
import random
import threading
import time

import config
from comun.protocolo import Conexion
from comun.registro import Registro


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default="1")
    ap.add_argument("--puerto-broker", type=int, default=config.PUERTO_BROKER)
    ap.add_argument("--version", choices=["v1", "v2"], default="v2")
    ap.add_argument("--hilos", type=int, default=None)
    ap.add_argument("--silencioso", action="store_true")
    args = ap.parse_args()

    perfil = config.VERSIONES[args.version]
    hilos = args.hilos or perfil["hilos"]
    ack = perfil["ack"]
    nombre = f"TRABAJADOR-{args.id}"
    log = Registro(nombre)
    azar = random.Random(hash(nombre))

    publicador = Conexion.conectar(config.HOST, args.puerto_broker)
    procesados = [0]
    cand = threading.Lock()

    def publicar(tema, msg):
        publicador.enviar({"op": "publicar", "tema": tema, "msg": msg})

    def procesar(pedido):
        # Simulacion de llamadas a otros sistemas (I/O): inventario, pago, logistica.
        for t in (config.T_VALIDAR, config.T_COBRAR, config.T_ASIGNAR):
            time.sleep(t * azar.uniform(0.8, 1.2))

    def consumidor(n):
        conn = Conexion.conectar(config.HOST, args.puerto_broker)
        while True:
            r = conn.solicitar({"op": "obtener", "cola": config.COLA_PEDIDOS, "ack": ack, "espera": 0.5})
            if r["id"] is None:
                continue
            pedido = r["msg"]
            publicar(config.TEMA_ESTADO, {"id": pedido["id"], "estado": "en_proceso", "trabajador": nombre, "ts": time.time()})
            procesar(pedido)
            publicar(config.TEMA_ESTADO, {"id": pedido["id"], "estado": "completado", "trabajador": nombre, "ts": time.time()})
            if ack:
                conn.enviar({"op": "ack", "id": r["id"]})
            with cand:
                procesados[0] += 1
                total = procesados[0]
            if not args.silencioso or total % 25 == 0:
                log.info(f"hilo {n}: pedido {pedido['id']} de {pedido['cliente']} completado (total {total})")

    def latido():
        while True:
            publicar(config.TEMA_HEARTBEAT, {"nodo": nombre, "ts": time.time(), "procesados": procesados[0], "hilos": hilos})
            time.sleep(config.INTERVALO_HEARTBEAT)

    log.info(f"Iniciado con {hilos} hilo(s), ack={'si' if ack else 'no'}")
    threading.Thread(target=latido, daemon=True).start()
    trabajadores = [threading.Thread(target=consumidor, args=(i + 1,), daemon=True) for i in range(hilos)]
    for t in trabajadores:
        t.start()
    for t in trabajadores:
        t.join()


if __name__ == "__main__":
    main()
