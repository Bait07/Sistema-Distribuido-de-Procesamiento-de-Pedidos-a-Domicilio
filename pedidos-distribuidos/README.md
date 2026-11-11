# Sistema Distribuido de Procesamiento de Pedidos a Domicilio

Prototipo del mini proyecto de la Unidad 2 del curso **Arquitectura de Software**
(*Tejiendo redes: arquitectura de software entre hilos y nodos*).

Autor: **Juan Diego Enríquez Cardona**. El proyecto fue desarrollado de forma
individual: el autor asumió y simuló todos los roles del equipo (líder de proyecto,
arquitecto, desarrollador y evaluador de calidad).

## Qué hace

Un cliente hace un pedido a domicilio por HTTP. El pedido atraviesa una red de
nodos independientes (procesos del sistema operativo que solo se comunican por TCP):

| Nodo | Patrón | Responsabilidad | Concurrencia |
|---|---|---|---|
| `gateway` | Proxy / API Gateway, Circuit Breaker | Punto único de entrada HTTP, validación, métricas | Un hilo por petición |
| `broker` | Mediator | Cola de trabajo con confirmación (ack) y publicación/suscripción | Un hilo por conexión |
| `trabajador` (N) | Productor Consumidor, Thread Pool | Valida inventario, cobra y asigna repartidor | Pool de hilos consumidores |
| `notificador` | Observer distribuido | Avisa al cliente cuando el pedido está listo | Suscriptor asíncrono |
| `monitor` | Heartbeat | Detecta nodos caídos | Hilo vigilante |

## Requisitos

* Python 3.10 o superior (no usa librerías externas para el sistema).
* `matplotlib` solo para generar gráficas: `pip install -r requirements.txt`.

## Cómo ejecutarlo

```bash
# 1. Demostración guiada (la usada en el video)
python demo.py

# 2. Levantar el cluster y dejarlo corriendo
python cluster.py --version v2 --trabajadores 3
#    en otra terminal:
python cliente_carga.py --pedidos 300 --concurrencia 16

# 3. Repetir la evaluación completa (unos 3 minutos)
python experimentos.py
python graficas.py
```

API del gateway (puerto 8080):

* `POST /pedidos` con `{"cliente": "Ana", "items": [{"producto": "Pizza", "cantidad": 2}], "total": 45000}` devuelve `202` y un `id`.
* `GET /pedidos/<id>` devuelve el estado del pedido.
* `GET /metricas` devuelve contadores, latencias y estado del circuito.

## Versiones

| | v1 (inicial) | v2 (optimizada) |
|---|---|---|
| Hilos por trabajador | 1 | 8 |
| Confirmación de mensajes (ack) | No | Sí, con reentrega si el nodo cae |
| Conexión gateway a broker | Nueva por pedido | Pool de conexiones persistentes |
| Circuit Breaker | No | Sí (3 fallas, 2 s de reposo) |
| TCP_NODELAY en el gateway | No | Sí |

## Resultados principales (ver `resultados/`)

* Con 1 nodo, v2 procesa 192 pedidos/s frente a 24 de v1 (8 veces más).
* v2 escala casi linealmente hasta 8 nodos (1 362 pedidos/s) y se satura cerca
  de 1 750 pedidos/s, limitada por la entrada (gateway y broker).
* Caída de un trabajador: v1 pierde los pedidos en vuelo; v2 pierde 0 (reentrega).
* Broker sin responder: v1 tarda 1 s en cada respuesta fallida; v2 responde en ~1 ms gracias al Circuit Breaker.

## Estructura

```
config.py            parámetros y perfiles v1 / v2
comun/               protocolo JSON sobre TCP, registro, Circuit Breaker
nodos/               broker, gateway, trabajador, notificador, monitor
cluster.py           lanza cada nodo como un proceso independiente
cliente_carga.py     generador de carga concurrente
experimentos.py      pruebas de escalabilidad y robustez
graficas.py          genera las figuras del informe
demo.py              demostración guiada para el video
uml/                 fuentes PlantUML de los diagramas
```
