import time
import numpy as np
import pandas as pd
from utils import guardar_log_csv

def construir_bloque(df, T_simulado, gas_limit=30_000_000):
    """    Construye un bloque utilizando un algoritmo greedy clásico  
    basado en la maximización de la
    utilidad de las transacciones, priorizando aquellas con mayor
    gas_fee_cap por unidad de gas.
    Parámetros:
        df (pd.DataFrame): DataFrame con las transacciones.
        T_simulado (int): Timestamp simulado de inclusión del bloque.
        gas_limit (int): Límite de gas del bloque (default: 30_000_000).
    Retorna:
        tuple: Resumen de la construcción del bloque y DataFrame con las transacciones incluidas.
    """

    inicio = time.perf_counter()
    bloque = []
    gas_usado = 0

    txs = df.to_dict("records")

    txs_ordenados = sorted(
        txs,
        key=lambda tx: (tx["gas_fee_cap"] * tx["gas"]) / tx["gas"],
        reverse=True
    )

    for tx in txs_ordenados:
        if any(
            tx["from"] == otro["from"] and tx.get("nonce") == otro.get("nonce")
            or tx["to"] == otro["to"] for otro in bloque
        ):
            continue

        if gas_usado + tx["gas"] <= gas_limit:
            bloque.append(tx)
            gas_usado += tx["gas"]

    fin = time.perf_counter()

    bloque_df = pd.DataFrame(bloque)

    if not bloque_df.empty:
        bloque_df["fee"] = bloque_df["gas"] * bloque_df["gas_fee_cap"]
        bloque_df["lead_time_ms"] = T_simulado - bloque_df["timestamp_ms"]

        utilidad_total = int(bloque_df["fee"].sum())
        lead_time_prom = round(bloque_df["lead_time_ms"].mean() / 1000, 3)
        gas_usado_total = int(bloque_df["gas"].sum())
    else:
        utilidad_total = 0
        lead_time_prom = 0.0
        gas_usado_total = 0

    resumen = {
        "algoritmo": "greedy_clasico",
        "timestamp_simulado": T_simulado,
        "total_transacciones": len(df),
        "tx_incluidas": len(bloque_df),
        "gas_usado": gas_usado_total,
        "utilidad_total": utilidad_total,
        "fragmentacion": gas_limit - gas_usado_total,
        "lead_time_promedio_s": lead_time_prom,
        "tiempo_ejecucion_s": round(fin - inicio, 4)
    }
    guardar_log_csv(resumen)

    return resumen, bloque_df
