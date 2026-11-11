"""Protocolo de comunicacion entre nodos.

Cada mensaje es un objeto JSON en una sola linea (JSON Lines) sobre TCP.
La clase Conexion encapsula el socket y protege la escritura con un candado,
porque varios hilos de un mismo nodo pueden publicar por la misma conexion.
"""
import json
import socket
import threading
import time


class Conexion:
    def __init__(self, sock):
        self.sock = sock
        self._lector = sock.makefile("r", encoding="utf-8")
        self._candado = threading.Lock()

    @classmethod
    def conectar(cls, host, puerto, timeout=None):
        sock = socket.create_connection((host, puerto), timeout=timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return cls(sock)

    def enviar(self, mensaje):
        datos = (json.dumps(mensaje, ensure_ascii=False) + "\n").encode("utf-8")
        with self._candado:
            self.sock.sendall(datos)

    def recibir(self):
        linea = self._lector.readline()
        if not linea:
            raise ConnectionError("conexion cerrada por el otro extremo")
        return json.loads(linea)

    def solicitar(self, mensaje):
        """Envia una peticion y espera su respuesta (patron peticion/respuesta)."""
        self.enviar(mensaje)
        return self.recibir()

    def cerrar(self):
        for accion in (self._lector.close, self.sock.close):
            try:
                accion()
            except OSError:
                pass


def esperar_puerto(host, puerto, timeout=10.0):
    """Espera hasta que un nodo empiece a escuchar en su puerto."""
    limite = time.time() + timeout
    while time.time() < limite:
        try:
            with socket.create_connection((host, puerto), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"el puerto {puerto} no respondio a tiempo")
