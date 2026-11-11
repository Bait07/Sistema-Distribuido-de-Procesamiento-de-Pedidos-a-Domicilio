"""Bateria de pruebas de escalabilidad y robustez.

E1  Escalabilidad horizontal: 1, 2, 4 y 8 nodos trabajadores (v1 y v2).
E2  Escalabilidad por hilos dentro de un nodo (v2, 1 trabajador).
E3  Robustez ante la caida de un trabajador (sin ack v1 frente a con ack v2).
E4  Robustez ante un broker que deja de responder (sin y con Circuit Breaker).

Uso:  python experimentos.py           (resultados en resultados/resultados.json)
      python experimentos.py --rapido  (menos pedidos, para probar)
"""
import argparse
import itertools
import json
import os
import platform
import threading
import time

from cluster import Cluster
from cliente_carga import generar_carga, esperar_completados, metricas
from comun.protocolo import Conexion
import config

RAIZ = os.path.dirname(os.path.abspath(__file__))
SALIDA = os.path.join(RAIZ, "resultados")
_puertos = itertools.count(0)


def puertos():
    i = next(_puertos)
    return 5600 + i, 8100 + i


def pct(valores, p):
    v = sorted(valores)
    return round(v[min(len(v) - 1, int(p * len(v)))] * 1000, 1) if v else None


def corrida(version, trabajadores, hilos, pedidos, concurrencia):
    pb, pg = puertos()
    with Cluster(version, trabajadores, hilos, pb, pg, dir_logs=os.path.join(RAIZ, "logs", f"{version}_{trabajadores}x{hilos}"), silencioso=True):
        res = generar_carga(pg, pedidos, concurrencia)
        m = esperar_completados(pg, timeout=120)
    dur = m["ultimo_completado"] - m["primer_aceptado"]
    return {
        "version": version, "trabajadores": trabajadores, "hilos": hilos,
        "pedidos": pedidos, "aceptados": m["aceptados"], "completados": m["completados"],
        "duracion_s": round(dur, 3),
        "throughput": round(m["completados"] / dur, 1),
        "lat_p50_ms": m["latencia_ms"]["p50"], "lat_p95_ms": m["latencia_ms"]["p95"],
        "resp_gateway_p95_ms": pct([r[1] for r in res], 0.95),
    }


def e1(pedidos):
    filas = []
    for version, hilos in (("v1", 1), ("v2", 8)):
        for n in (1, 2, 4, 8):
            r = corrida(version, n, hilos, pedidos, 32)
            print("E1", r)
            filas.append(r)
    return filas


def e2(pedidos):
    filas = []
    for h in (1, 2, 4, 8, 16, 32):
        r = corrida("v2", 1, h, pedidos, 32)
        print("E2", r)
        filas.append(r)
    return filas


def e3(pedidos):
    filas = []
    for version in ("v1", "v2"):
        pb, pg = puertos()
        eventos = os.path.join(SALIDA, f"monitor_{version}.jsonl")
        if os.path.exists(eventos):
            os.remove(eventos)
        with Cluster(version, 4, 4, pb, pg, dir_logs=os.path.join(RAIZ, "logs", f"caida_{version}"),
                     silencioso=True, eventos_monitor=eventos) as c:
            hilo = threading.Thread(target=generar_carga, args=(pg, pedidos, 32))
            hilo.start()
            time.sleep(0.6)
            t_caida = time.time()
            c.matar("trabajador-2")
            hilo.join()
            m = esperar_completados(pg, timeout=30, estable=3)
            conn = Conexion.conectar(config.HOST, pb)
            stats = conn.solicitar({"op": "stats"})
            conn.cerrar()
        deteccion = None
        if os.path.exists(eventos):
            with open(eventos, encoding="utf-8") as f:
                for linea in f:
                    ev = json.loads(linea)
                    if ev["estado"] == "CAIDO":
                        deteccion = round(ev["detectado"] - t_caida, 2)
        r = {"version": version, "aceptados": m["aceptados"], "completados": m["completados"],
             "perdidos": m["aceptados"] - m["completados"], "reentregados": stats.get("reentregados", 0),
             "deteccion_s": deteccion, "lat_p95_ms": m["latencia_ms"]["p95"]}
        print("E3", r)
        filas.append(r)
    return filas


def e4():
    filas, series = [], {}
    for version in ("v1", "v2"):
        pb, pg = puertos()
        with Cluster(version, 2, 4, pb, pg, dir_logs=os.path.join(RAIZ, "logs", f"cb_{version}"), silencioso=True) as c:
            inicio = time.time()
            def falla():
                time.sleep(1.5)
                c.pausar_broker(3.0)
            threading.Thread(target=falla).start()
            res = generar_carga(pg, None, 8, duracion=7.0, pausa=0.02)
            m = metricas(pg)
        fallidas = [r for r in res if r[2] != 202]
        ventana = [r for r in res if 1.5 <= r[0] - inicio <= 4.5]
        series[version] = [(round(r[0] - inicio, 3), round(r[1] * 1000, 1), r[2]) for r in sorted(res)]
        r = {"version": version, "peticiones": len(res), "exitosas": len(res) - len(fallidas),
             "fallidas_503": len(fallidas),
             "resp_falla_prom_ms": round(sum(x[1] for x in fallidas) / len(fallidas) * 1000, 1) if fallidas else None,
             "resp_p95_durante_falla_ms": pct([x[1] for x in ventana], 0.95),
             "peticiones_durante_falla": len(ventana),
             "circuito_final": m["circuito"]}
        print("E4", r)
        filas.append(r)
    return filas, series


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rapido", action="store_true")
    args = ap.parse_args()
    n = 200 if args.rapido else 600
    os.makedirs(SALIDA, exist_ok=True)
    t0 = time.time()
    datos = {
        "entorno": {"python": platform.python_version(), "so": platform.platform(),
                    "cpus": os.cpu_count(), "fecha": time.strftime("%Y-%m-%d %H:%M"),
                    "tiempo_por_pedido_ms": round((config.T_VALIDAR + config.T_COBRAR + config.T_ASIGNAR) * 1000)},
        "E1_escalabilidad_nodos": e1(n),
        "E2_escalabilidad_hilos": e2(n),
        "E3_caida_trabajador": e3(n + 200),
    }
    datos["E4_circuit_breaker"], datos["E4_series"] = e4()
    with open(os.path.join(SALIDA, "resultados.json"), "w", encoding="utf-8") as f:
        json.dump(datos, f, indent=2, ensure_ascii=False)
    print(f"\nListo en {time.time() - t0:.0f} s. Resultados en resultados/resultados.json")


if __name__ == "__main__":
    main()
