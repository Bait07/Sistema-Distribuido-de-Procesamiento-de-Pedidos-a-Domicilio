"""Nodo NOTIFICADOR (Observer distribuido).

Se suscribe al tema de estados de pedido y "notifica" al cliente cuando su
pedido queda listo. No sabe quien produjo el evento: solo reacciona a el.
"""
import argparse
import time

import config
from comun.protocolo import Conexion
from comun.registro import Registro

log = Registro("NOTIFICADOR")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--puerto-broker", type=int, default=config.PUERTO_BROKER)
    ap.add_argument("--silencioso", action="store_true")
    args = ap.parse_args()

    enviadas = 0
    log.info("Suscrito a eventos de estado de pedidos")
    while True:
        try:
            conn = Conexion.conectar(config.HOST, args.puerto_broker)
            conn.solicitar({"op": "suscribir", "tema": config.TEMA_ESTADO})
            conn.solicitar({"op": "suscribir", "tema": config.TEMA_NODOS})
            while True:
                ev = conn.recibir()
                msg = ev["msg"]
                if ev["tema"] == config.TEMA_NODOS:
                    log.info(f"Alerta operativa: {msg['nodo']} {msg['estado']}")
                elif msg["estado"] == "completado":
                    enviadas += 1
                    if not args.silencioso or enviadas % 50 == 0:
                        log.info(f"Notificacion #{enviadas}: pedido {msg['id']} listo (atendido por {msg['trabajador']})")
        except (ConnectionError, OSError, ValueError):
            time.sleep(0.3)


if __name__ == "__main__":
    main()
