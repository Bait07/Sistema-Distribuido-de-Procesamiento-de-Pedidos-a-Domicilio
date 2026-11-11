"""Lanzador del cluster: cada nodo corre como un PROCESO independiente del
sistema operativo y se comunica con los demas por red (TCP en localhost).

Uso:
    python cluster.py --version v2 --trabajadores 3
(Ctrl+C para detener todos los nodos)
"""
import argparse
import os
import subprocess
import sys
import time
import urllib.request

import config
from comun.protocolo import Conexion, esperar_puerto

RAIZ = os.path.dirname(os.path.abspath(__file__))


class Cluster:
    def __init__(self, version="v2", trabajadores=2, hilos=None, puerto_broker=config.PUERTO_BROKER,
                 puerto_gateway=config.PUERTO_GATEWAY, dir_logs=None, silencioso=False, eventos_monitor=None):
        self.version = version
        self.n_trabajadores = trabajadores
        self.hilos = hilos
        self.pb = puerto_broker
        self.pg = puerto_gateway
        self.dir_logs = dir_logs
        self.silencioso = silencioso
        self.eventos_monitor = eventos_monitor
        self.procesos = {}
        self._archivos = []

    def _lanzar(self, nombre, modulo, *extra):
        cmd = [sys.executable, "-u", "-m", f"nodos.{modulo}", *map(str, extra)]
        salida = None
        if self.dir_logs:
            os.makedirs(self.dir_logs, exist_ok=True)
            salida = open(os.path.join(self.dir_logs, f"{nombre}.log"), "w", encoding="utf-8")
            self._archivos.append(salida)
        self.procesos[nombre] = subprocess.Popen(cmd, cwd=RAIZ, stdout=salida, stderr=subprocess.STDOUT if salida else None)

    def iniciar(self):
        self._lanzar("broker", "broker", "--puerto", self.pb)
        esperar_puerto(config.HOST, self.pb)
        sil = ["--silencioso"] if self.silencioso else []
        self._lanzar("notificador", "notificador", "--puerto-broker", self.pb, *sil)
        mon = ["--eventos", self.eventos_monitor] if self.eventos_monitor else []
        self._lanzar("monitor", "monitor", "--puerto-broker", self.pb, *mon)
        self._lanzar("gateway", "gateway", "--puerto", self.pg, "--puerto-broker", self.pb, "--version", self.version)
        for i in range(1, self.n_trabajadores + 1):
            self.agregar_trabajador(i)
        esperar_puerto(config.HOST, self.pg)
        time.sleep(0.8)
        return self

    def agregar_trabajador(self, i):
        hilos = ["--hilos", self.hilos] if self.hilos else []
        sil = ["--silencioso"] if self.silencioso else []
        self._lanzar(f"trabajador-{i}", "trabajador", "--id", i, "--puerto-broker", self.pb,
                     "--version", self.version, *hilos, *sil)

    def matar(self, nombre):
        """Simula la caida abrupta de un nodo (sin apagado ordenado)."""
        self.procesos[nombre].kill()

    def pausar_broker(self, segundos):
        conn = Conexion.conectar(config.HOST, self.pb, timeout=2)
        conn.solicitar({"op": "pausar", "segundos": segundos})
        conn.cerrar()

    def metricas(self):
        with urllib.request.urlopen(f"http://{config.HOST}:{self.pg}/metricas", timeout=5) as r:
            import json
            return json.loads(r.read())

    def detener(self):
        for p in self.procesos.values():
            if p.poll() is None:
                p.terminate()
        for p in self.procesos.values():
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
        for f in self._archivos:
            f.close()

    def __enter__(self):
        return self.iniciar()

    def __exit__(self, *exc):
        self.detener()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", choices=["v1", "v2"], default="v2")
    ap.add_argument("--trabajadores", type=int, default=3)
    ap.add_argument("--hilos", type=int, default=None)
    args = ap.parse_args()
    c = Cluster(args.version, args.trabajadores, args.hilos)
    try:
        c.iniciar()
        print(f"\nCluster {args.version} listo. Gateway: http://{config.HOST}:{config.PUERTO_GATEWAY}  (Ctrl+C para detener)\n")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        c.detener()


if __name__ == "__main__":
    main()
