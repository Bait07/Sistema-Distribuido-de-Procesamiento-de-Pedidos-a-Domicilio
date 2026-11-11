"""Registro (log) sencillo con el nombre del nodo como prefijo."""
import sys
import threading
import time

_candado = threading.Lock()


class Registro:
    def __init__(self, nodo):
        self.nodo = nodo

    def info(self, texto):
        marca = time.strftime("%H:%M:%S") + f".{int(time.time() * 1000) % 1000:03d}"
        with _candado:
            sys.stdout.write(f"{marca} [{self.nodo}] {texto}\n")
            sys.stdout.flush()
