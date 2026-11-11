"""Patron Circuit Breaker (interruptor de circuito).

Protege a quien llama frente a un servicio remoto que no responde.
Estados:
  CERRADO     las llamadas pasan normalmente.
  ABIERTO     tras `umbral_fallas` fallas seguidas se rechaza de inmediato,
              sin esperar el tiempo de espera de la red.
  SEMIABIERTO pasado `tiempo_reset` se deja pasar una llamada de prueba;
              si funciona el circuito se cierra, si falla se vuelve a abrir.
"""
import threading
import time

CERRADO = "CERRADO"
ABIERTO = "ABIERTO"
SEMIABIERTO = "SEMIABIERTO"


class CircuitoAbierto(Exception):
    pass


class CircuitBreaker:
    def __init__(self, umbral_fallas=3, tiempo_reset=2.0, registro=None):
        self.umbral_fallas = umbral_fallas
        self.tiempo_reset = tiempo_reset
        self.estado = CERRADO
        self.fallas = 0
        self.abierto_en = 0.0
        self.prueba_en_curso = False
        self.aperturas = 0
        self._candado = threading.Lock()
        self._registro = registro

    def _cambiar(self, nuevo):
        if nuevo != self.estado and self._registro:
            self._registro.info(f"Circuit breaker: {self.estado} -> {nuevo}")
        self.estado = nuevo

    def llamar(self, funcion, *args, **kwargs):
        with self._candado:
            if self.estado == ABIERTO:
                if time.time() - self.abierto_en >= self.tiempo_reset:
                    self._cambiar(SEMIABIERTO)
                else:
                    raise CircuitoAbierto("circuito abierto")
            if self.estado == SEMIABIERTO:
                if self.prueba_en_curso:
                    raise CircuitoAbierto("circuito en prueba")
                self.prueba_en_curso = True
        try:
            resultado = funcion(*args, **kwargs)
        except Exception:
            with self._candado:
                self.prueba_en_curso = False
                self.fallas += 1
                if self.estado == SEMIABIERTO or self.fallas >= self.umbral_fallas:
                    if self.estado != ABIERTO:
                        self.aperturas += 1
                    self._cambiar(ABIERTO)
                    self.abierto_en = time.time()
            raise
        with self._candado:
            self.prueba_en_curso = False
            self.fallas = 0
            self._cambiar(CERRADO)
        return resultado
