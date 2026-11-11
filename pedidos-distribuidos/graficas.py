"""Genera las graficas de la evaluacion a partir de resultados/resultados.json.

Uso:  python graficas.py   (requiere matplotlib: pip install matplotlib)
"""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RAIZ = os.path.dirname(os.path.abspath(__file__))
DIR = os.path.join(RAIZ, "resultados", "graficas")
C_V1, C_V2 = "#D9622B", "#2E5C9A"   # paleta validada (contraste y daltonismo)
TINTA, TINTA2, REJILLA = "#1F2933", "#52606D", "#E4E7EB"

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10, "axes.edgecolor": REJILLA,
    "axes.labelcolor": TINTA2, "xtick.color": TINTA2, "ytick.color": TINTA2,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
    "grid.color": REJILLA, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlecolor": TINTA,
    "figure.dpi": 200, "savefig.bbox": "tight",
})


def guardar(fig, nombre):
    os.makedirs(DIR, exist_ok=True)
    fig.savefig(os.path.join(DIR, nombre), facecolor="white")
    plt.close(fig)


def etiqueta_final(ax, x, y, texto, color):
    ax.annotate(texto, (x, y), xytext=(6, 0), textcoords="offset points", va="center", color=TINTA, fontsize=9,
                fontweight="bold")
    ax.plot([], [], color=color)


def main():
    d = json.load(open(os.path.join(RAIZ, "resultados", "resultados.json"), encoding="utf-8"))

    # 1. Escalabilidad horizontal
    e1 = d["E1_escalabilidad_nodos"]
    fig, ax = plt.subplots(figsize=(7, 4))
    for v, c, nom in (("v1", C_V1, "v1 (1 hilo por nodo)"), ("v2", C_V2, "v2 (8 hilos por nodo)")):
        f = [r for r in e1 if r["version"] == v]
        x = [r["trabajadores"] for r in f]
        y = [r["throughput"] for r in f]
        ax.plot(x, y, color=c, lw=2, marker="o", ms=7, label=nom, markeredgecolor="white", markeredgewidth=1.5)
        for xi, yi in zip(x, y):
            ax.annotate(f"{yi:.0f}", (xi, yi), xytext=(0, 8), textcoords="offset points", ha="center", fontsize=8, color=TINTA2)
    ax.set_xticks([1, 2, 4, 8])
    ax.set_xlabel("Nodos trabajadores")
    ax.set_ylabel("Pedidos completados por segundo")
    ax.set_title("Throughput según el número de nodos", loc="left")
    ax.legend(frameon=False, loc="upper left")
    guardar(fig, "fig1_escalabilidad_nodos.png")

    # 2. Latencia p95 (escala logaritmica)
    fig, ax = plt.subplots(figsize=(7, 4))
    for v, c, nom in (("v1", C_V1, "v1"), ("v2", C_V2, "v2")):
        f = [r for r in e1 if r["version"] == v]
        ax.plot([r["trabajadores"] for r in f], [r["lat_p95_ms"] for r in f], color=c, lw=2, marker="o", ms=7,
                label=nom, markeredgecolor="white", markeredgewidth=1.5)
    ax.set_yscale("log")
    ax.set_xticks([1, 2, 4, 8])
    ax.set_xlabel("Nodos trabajadores")
    ax.set_ylabel("Latencia p95 de punta a punta (ms, escala log)")
    ax.set_title("Latencia p95 según el número de nodos", loc="left")
    ax.legend(frameon=False)
    guardar(fig, "fig2_latencia_p95.png")

    # 3. Escalabilidad por hilos
    e2 = d["E2_escalabilidad_hilos"]
    fig, ax = plt.subplots(figsize=(7, 4))
    x = [r["hilos"] for r in e2]
    ax.plot(x, [h / 0.040 for h in x], color="#9AA5B1", lw=1.5, ls="--", label="Ideal (hilos / 40 ms)")
    ax.plot(x, [r["throughput"] for r in e2], color=C_V2, lw=2, marker="o", ms=7, label="Medido",
            markeredgecolor="white", markeredgewidth=1.5)
    ax.set_xscale("log", base=2)
    ax.set_xticks(x)
    ax.set_xticklabels(x)
    ax.set_xlabel("Hilos en un único nodo trabajador")
    ax.set_ylabel("Pedidos completados por segundo")
    ax.set_title("Throughput según el tamaño del pool de hilos", loc="left")
    ax.legend(frameon=False)
    guardar(fig, "fig3_escalabilidad_hilos.png")

    # 4. Circuit breaker: tiempo de respuesta en el tiempo (dos paneles, mismo eje)
    series = d["E4_series"]
    fig, axes = plt.subplots(2, 1, figsize=(7, 5), sharex=True, sharey=True)
    for ax, v, c, nom in ((axes[0], "v1", C_V1, "v1 sin Circuit Breaker"), (axes[1], "v2", C_V2, "v2 con Circuit Breaker")):
        ok = [(t, ms) for t, ms, cod in series[v] if cod == 202]
        mal = [(t, ms) for t, ms, cod in series[v] if cod != 202]
        ax.axvspan(1.5, 4.5, color="#F5E6DA", zorder=0)
        ax.scatter([p[0] for p in ok], [max(p[1], 0.3) for p in ok], s=8, color=c, alpha=0.6, label="202 aceptado", lw=0)
        ax.scatter([p[0] for p in mal], [max(p[1], 0.3) for p in mal], s=14, marker="x", color=TINTA, label="503 rechazado", lw=1)
        ax.set_yscale("log")
        ax.set_title(nom, loc="left", fontsize=10)
        ax.set_ylabel("Respuesta (ms)")
    axes[0].text(3.0, 3000, "broker sin responder", ha="center", fontsize=8, color=TINTA2)
    axes[1].set_xlabel("Segundos desde el inicio de la prueba")
    axes[0].legend(frameon=False, loc="upper right", fontsize=8)
    fig.suptitle("Tiempo de respuesta del gateway ante la falla del broker", x=0.02, ha="left", fontweight="bold", color=TINTA)
    guardar(fig, "fig4_circuit_breaker.png")

    # 5. Saturacion
    e5 = d.get("E5_saturacion")
    if e5:
        fig, ax = plt.subplots(figsize=(7, 4))
        etiquetas = [f"{r['trabajadores']}×{r['hilos']}" for r in e5]
        xs = range(len(e5))
        ax.bar([i - 0.2 for i in xs], [r["teorico"] for r in e5], width=0.38, color="#CBD2D9", label="Capacidad teórica de los trabajadores")
        ax.bar([i + 0.2 for i in xs], [r["throughput"] for r in e5], width=0.38, color=C_V2, label="Throughput medido")
        for i, r in enumerate(e5):
            ax.annotate(f"{r['throughput']:.0f}", (i + 0.2, r["throughput"]), xytext=(0, 4), textcoords="offset points", ha="center", fontsize=8, color=TINTA)
        ax.set_xticks(list(xs))
        ax.set_xticklabels(etiquetas)
        ax.set_xlabel("Nodos × hilos por nodo")
        ax.set_ylabel("Pedidos por segundo")
        ax.set_title("Saturación: la entrada limita el sistema", loc="left")
        ax.legend(frameon=False)
        guardar(fig, "fig5_saturacion.png")
    print("Graficas en", DIR)


if __name__ == "__main__":
    main()
