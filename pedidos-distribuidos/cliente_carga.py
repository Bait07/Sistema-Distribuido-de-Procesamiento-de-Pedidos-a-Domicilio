"""Generador de carga: simula muchos clientes concurrentes enviando pedidos.

Uso:
    python cliente_carga.py --pedidos 200 --concurrencia 10
"""
import argparse
import http.client
import itertools
import json
import random
import threading
import time
import urllib.request

import config

PRODUCTOS = ["Hamburguesa", "Pizza", "Arepa", "Bandeja paisa", "Empanadas", "Jugo natural", "Ensalada"]
CLIENTES = ["Ana", "Carlos", "Luisa", "Mateo", "Valentina", "Santiago", "Camila", "Andres"]


def pedido_aleatorio(azar):
    items = [{"producto": azar.choice(PRODUCTOS), "cantidad": azar.randint(1, 3)} for _ in range(azar.randint(1, 3))]
    return {"cliente": azar.choice(CLIENTES), "items": items, "total": round(azar.uniform(15000, 90000), 0)}


def generar_carga(puerto=config.PUERTO_GATEWAY, total=None, concurrencia=10, duracion=None, pausa=0.0, semilla=7):
    """Envia `total` pedidos (o durante `duracion` segundos). Devuelve una lista de
    (instante_envio, tiempo_respuesta_s, codigo_http)."""
    contador = itertools.count()
    resultados = []
    cand = threading.Lock()
    fin = time.time() + duracion if duracion else None

    def cliente(n):
        azar = random.Random(semilla + n)
        conn = http.client.HTTPConnection(config.HOST, puerto, timeout=10)
        while True:
            if total is not None and next(contador) >= total:
                break
            if fin is not None and time.time() >= fin:
                break
            cuerpo = json.dumps(pedido_aleatorio(azar))
            t0 = time.time()
            try:
                conn.request("POST", "/pedidos", cuerpo, {"Content-Type": "application/json"})
                r = conn.getresponse()
                r.read()
                codigo = r.status
            except (OSError, http.client.HTTPException):
                codigo = 0
                conn.close()
                conn = http.client.HTTPConnection(config.HOST, puerto, timeout=10)
            with cand:
                resultados.append((t0, time.time() - t0, codigo))
            if pausa:
                time.sleep(pausa)
        conn.close()

    hilos = [threading.Thread(target=cliente, args=(i,)) for i in range(concurrencia)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    return resultados


def metricas(puerto=config.PUERTO_GATEWAY):
    with urllib.request.urlopen(f"http://{config.HOST}:{puerto}/metricas", timeout=5) as r:
        return json.loads(r.read())


def esperar_completados(puerto=config.PUERTO_GATEWAY, timeout=60, estable=None):
    """Espera hasta que completados == aceptados, o hasta que el conteo deje de
    cambiar durante `estable` segundos, o hasta `timeout`."""
    limite = time.time() + timeout
    ultimo, desde = -1, time.time()
    while time.time() < limite:
        m = metricas(puerto)
        if m["completados"] >= m["aceptados"]:
            return m
        if m["completados"] != ultimo:
            ultimo, desde = m["completados"], time.time()
        elif estable and time.time() - desde > estable:
            return m
        time.sleep(0.1)
    return metricas(puerto)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pedidos", type=int, default=200)
    ap.add_argument("--concurrencia", type=int, default=10)
    ap.add_argument("--puerto", type=int, default=config.PUERTO_GATEWAY)
    args = ap.parse_args()
    t0 = time.time()
    res = generar_carga(args.puerto, args.pedidos, args.concurrencia)
    ok = sum(1 for r in res if r[2] == 202)
    print(f"Enviados {len(res)} pedidos en {time.time() - t0:.2f} s, aceptados {ok}")
    m = esperar_completados(args.puerto)
    dur = (m["ultimo_completado"] or t0) - (m["primer_aceptado"] or t0)
    print(json.dumps(m, indent=2, ensure_ascii=False))
    if dur > 0:
        print(f"Throughput: {m['completados'] / dur:.1f} pedidos/s")


if __name__ == "__main__":
    main()
