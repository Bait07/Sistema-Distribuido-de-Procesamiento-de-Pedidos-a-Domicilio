"""Demostracion guiada para el video (unos 3 minutos).

Muestra en una sola consola los registros de todos los nodos:
  1. Arranque del cluster (broker, gateway, notificador, monitor, 3 trabajadores).
  2. Envio de pedidos y su recorrido por los nodos e hilos.
  3. Caida abrupta de un trabajador: el monitor la detecta y el broker
     reentrega los pedidos sin confirmar (no se pierde ninguno).
  4. El broker deja de responder: el Circuit Breaker se abre y luego se cierra.
  5. Metricas finales del gateway.

Uso:  python demo.py            (pulse Enter entre pasos)
      python demo.py --auto     (sin pausas)
"""
import argparse
import json
import threading
import time

import random
import urllib.request

import config
from cluster import Cluster
from cliente_carga import generar_carga, metricas, pedido_aleatorio

URL = f"http://{config.HOST}:{config.PUERTO_GATEWAY}"


def http(metodo, ruta, cuerpo=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(URL + ruta, data=datos, method=metodo, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def titulo(texto, auto):
    print("\n" + "=" * 78 + f"\n  {texto}\n" + "=" * 78, flush=True)
    if not auto:
        input("  (Enter para continuar) ")
    else:
        time.sleep(1.5)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--auto", action="store_true")
    args = ap.parse_args()

    c = Cluster("v2", trabajadores=3, hilos=4, silencioso=True)
    try:
        titulo("PASO 1. Arranque del cluster: cada nodo es un proceso independiente", args.auto)
        c.iniciar()
        time.sleep(1)

        titulo("PASO 2. Un cliente envia pedidos al API Gateway (POST /pedidos)", args.auto)
        azar = random.Random(3)
        ids = []
        for _ in range(6):
            pedido = pedido_aleatorio(azar)
            codigo, r = http("POST", "/pedidos", pedido)
            productos = ", ".join(f"{i['cantidad']} {i['producto']}" for i in pedido["items"])
            print(f"  CLIENTE -> {pedido['cliente']:<10} pide {productos:<45} HTTP {codigo} id={r['id']}", flush=True)
            ids.append(r["id"])
        codigo, r = http("POST", "/pedidos", {"cliente": "", "items": [], "total": 0})
        print(f"  CLIENTE -> pedido invalido (sin cliente ni productos)                        HTTP {codigo} {r['error']}", flush=True)
        time.sleep(0.5)
        print("\n  Consulta de estado (GET /pedidos/<id>):", flush=True)
        for pid in ids:
            _, r = http("GET", f"/pedidos/{pid}")
            print(f"    {pid}: {r['estado']:<11} atendido por {r.get('trabajador')}  "
                  f"en {(r['completado'] - r['creado']) * 1000:.0f} ms", flush=True)
        time.sleep(1)

        titulo("PASO 3. Carga continua y caida abrupta de TRABAJADOR-2", args.auto)
        carga = threading.Thread(target=generar_carga, args=(config.PUERTO_GATEWAY, 500, 16))
        carga.start()
        time.sleep(0.6)
        print("\n>>> Matando el proceso TRABAJADOR-2 (simula un servidor que se apaga) <<<\n", flush=True)
        c.matar("trabajador-2")
        carga.join()
        time.sleep(2.5)
        m = metricas()
        print(f"\n>>> Aceptados: {m['aceptados']}  completados: {m['completados']}  perdidos: {m['aceptados'] - m['completados']}\n", flush=True)

        titulo("PASO 4. El broker deja de responder 3 s: Circuit Breaker en accion", args.auto)
        threading.Thread(target=lambda: (time.sleep(0.5), c.pausar_broker(3.0))).start()
        res = generar_carga(config.PUERTO_GATEWAY, None, 2, duracion=6.0, pausa=0.2)
        rechazados = sum(1 for r in res if r[2] == 503)
        print(f"\n>>> Peticiones durante la prueba: {len(res)}, rechazadas rapido con 503: {rechazados}\n", flush=True)
        time.sleep(1.5)

        titulo("PASO 5. Metricas finales del gateway", args.auto)
        m = metricas()
        print(json.dumps(m, indent=2, ensure_ascii=False))
        print(f"\nPedidos aceptados: {m['aceptados']}  completados: {m['completados']}  "
              f"perdidos: {m['aceptados'] - m['completados']}", flush=True)
    finally:
        c.detener()
        print("\nCluster detenido.")


if __name__ == "__main__":
    main()
