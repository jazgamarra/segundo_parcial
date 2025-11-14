import time
import numpy as np
import pandas as pd

from utils import guardar_log_csv, calcular_utilidad

def construir_bloque(df, T_simulado, gas_limit=30_000_000, top_n=200):
    import time
    from itertools import combinations

    inicio = time.perf_counter()
    df["fee"] = df["gas"] * df["gas_fee_cap"]
    txs_ordenadas = df.sort_values("fee", ascending=False).head(top_n).reset_index(drop=True)
    txs = txs_ordenadas.to_dict("records")

    combinaciones_validas = []
    for i, j in combinations(range(len(txs)), 2):
        ti, tj = txs[i], txs[j]
        gas_total = ti["gas"] + tj["gas"]
        if gas_total > gas_limit:
            continue
        utilidad = calcular_utilidad(ti, tj, gas_limit=gas_limit)
        addrs = {ti["from"], ti["to"], tj["from"], tj["to"]}
        combinaciones_validas.append({
            "i": i,
            "j": j,
            "utilidad_total": utilidad,
            "gas_total": gas_total,
            "addrs": addrs
        })

    combinaciones_ordenadas = sorted(combinaciones_validas, key=lambda x: x["utilidad_total"], reverse=True)

    bloque_idx = set()
    direcciones_ocupadas = set()
    gas_usado = 0
    for combo in combinaciones_ordenadas:
        if combo["addrs"] & direcciones_ocupadas:
            continue
        if gas_usado + combo["gas_total"] > gas_limit:
            continue
        bloque_idx.update([combo["i"], combo["j"]])
        gas_usado += combo["gas_total"]
        direcciones_ocupadas |= combo["addrs"]

    fin = time.perf_counter()
    bloque_df = pd.DataFrame([txs[i] for i in bloque_idx])
    bloque_df["lead_time_ms"] = T_simulado - bloque_df["timestamp_ms"]

    resumen = {
        "algoritmo": f"algoritmo_base",
        "timestamp_simulado": T_simulado,
        "total_transacciones": len(df),
        "tx_incluidas": len(bloque_df),
        "gas_usado": int(bloque_df["gas"].sum()),
        "utilidad_total": int(bloque_df["fee"].sum()),
        "fragmentacion": int(gas_limit - bloque_df["gas"].sum()),
        "lead_time_promedio_s": round(bloque_df["lead_time_ms"].mean() / 1000, 3),
        "tiempo_ejecucion_s": round(fin - inicio, 4)
    }

    guardar_log_csv(resumen)

    return resumen, bloque_df
