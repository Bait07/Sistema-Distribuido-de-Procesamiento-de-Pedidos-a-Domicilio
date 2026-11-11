"""Configuracion central del sistema distribuido de pedidos."""

HOST = "127.0.0.1"
PUERTO_BROKER = 5500
PUERTO_GATEWAY = 8080

COLA_PEDIDOS = "pedidos"
TEMA_ESTADO = "pedido.estado"
TEMA_HEARTBEAT = "heartbeat"
TEMA_NODOS = "nodo.estado"

# Tiempos simulados de cada etapa del procesamiento (segundos).
# Representan llamadas a inventario, pasarela de pago y asignacion de repartidor.
T_VALIDAR = 0.005
T_COBRAR = 0.020
T_ASIGNAR = 0.015

INTERVALO_HEARTBEAT = 0.5
LIMITE_SIN_HEARTBEAT = 1.5

# Version 1 (inicial) y version 2 (optimizada tras la evaluacion)
VERSIONES = {
    "v1": {"hilos": 1, "ack": False, "pool": False, "circuit_breaker": False, "nodelay": False},
    "v2": {"hilos": 8, "ack": True, "pool": True, "circuit_breaker": True, "nodelay": True},
}
