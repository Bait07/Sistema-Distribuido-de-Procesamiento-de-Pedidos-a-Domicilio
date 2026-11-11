"""Nodo MONITOR (deteccion de fallas por latidos).

Escucha los heartbeat de los trabajadores. Si un nodo deja de latir por mas de
LIMITE_SIN_HEARTBEAT segundos lo marca como caido y publica una alerta.
"""
import argparse
import json
import threading
import time

import config
from comun.protocolo import Conexion
from comun.registro import Registro

log = Registro("MONITOR")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--puerto-broker", type=int, default=config.PUERTO_BROKER)
    ap.add_argument("--eventos", default=None, help="archivo JSONL donde registrar caidas")
    args = ap.parse_args()

    ultimo = {}
    caidos = set()
    cand = threading.Lock()
    publicador = Conexion.conectar(config.HOST, args.puerto_broker)

    def registrar(evento):
        log.info(f"{evento['nodo']} -> {evento['estado']}")
        publicador.enviar({"op": "publicar", "tema": config.TEMA_NODOS, "msg": evento})
        if args.eventos:
            with open(args.eventos, "a", encoding="utf-8") as f:
                f.write(json.dumps(evento) + "\n")

    def vigilar():
        while True:
            ahora = time.time()
            with cand:
                for nodo, ts in list(ultimo.items()):
                    if ahora - ts > config.LIMITE_SIN_HEARTBEAT and nodo not in caidos:
                        caidos.add(nodo)
                        registrar({"nodo": nodo, "estado": "CAIDO", "ultimo_latido": ts, "detectado": ahora})
            time.sleep(0.1)

    threading.Thread(target=vigilar, daemon=True).start()
    log.info("Vigilando latidos de los trabajadores")
    conn = Conexion.conectar(config.HOST, args.puerto_broker)
    conn.solicitar({"op": "suscribir", "tema": config.TEMA_HEARTBEAT})
    while True:
        hb = conn.recibir()["msg"]
        with cand:
            nuevo = hb["nodo"] not in ultimo
            ultimo[hb["nodo"]] = hb["ts"]
            if hb["nodo"] in caidos:
                caidos.discard(hb["nodo"])
                registrar({"nodo": hb["nodo"], "estado": "RECUPERADO", "detectado": time.time()})
        if nuevo:
            log.info(f"Nuevo nodo registrado: {hb['nodo']} ({hb['hilos']} hilos)")


if __name__ == "__main__":
    main()
