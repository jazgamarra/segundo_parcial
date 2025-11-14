import pandas as pd
import time
from itertools import combinations
from utils import guardar_log_csv, calcular_utilidad

def _to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _safe_int(x):
    try:
        if pd.isna(x):
            return 0
        return int(x)
    except Exception:
        return 0

def _series_sum_int(s):
    if s is None:
        return 0
    return _safe_int(pd.to_numeric(s, errors="coerce").fillna(0).sum())

def construir_bloque(df, T_simulado, gas_limit=30_000_000, top_n=300, max_trios=10000, max_pares=20000):
    """
    Construye un bloque heurístico combinando tríos, pares y relleno greedy agresivo,
    con manejo robusto de NaN/strings en las columnas del dataset.
    """
    inicio = time.perf_counter()

    # --- Normalizar tipos y columnas mínimas ---
    df = df.copy()
    df = _to_numeric(df, ["gas", "gas_fee_cap", "timestamp_ms"])
    # Si falta timestamp_ms, créalo con T_simulado (para no romper métricas)
    if "timestamp_ms" not in df.columns:
        df["timestamp_ms"] = T_simulado

    # fee = gas * gas_fee_cap (robusto)
    df["fee"] = pd.to_numeric(df.get("gas", 0), errors="coerce") * pd.to_numeric(df.get("gas_fee_cap", 0), errors="coerce")

    # Limpieza mínima: gas y gas_fee_cap no-negativos
    df["gas"] = df["gas"].fillna(0).clip(lower=0)
    df["gas_fee_cap"] = df["gas_fee_cap"].fillna(0).clip(lower=0)
    df["fee"] = df["fee"].fillna(0).clip(lower=0)

    # Top-N por fee
    top_df = df.sort_values("fee", ascending=False).head(top_n)
    top_hashes = set(top_df.get("hash", []))
    top_from = set(top_df.get("from", [])) if "from" in df.columns else set()
    top_to   = set(top_df.get("to", []))   if "to" in df.columns else set()

    # Ampliar con relacionadas (si existen columnas)
    if {"from", "to", "hash"}.issubset(df.columns):
        relacionadas = df[
            (~df["hash"].isin(top_hashes)) & (
                df["from"].isin(top_from) | df["to"].isin(top_to)
            )
        ]
    else:
        relacionadas = df.iloc[0:0]  # vacío si no hay columnas

    ampliado_df = pd.concat([top_df, relacionadas], ignore_index=True)
    if "hash" in ampliado_df.columns:
        ampliado_df = ampliado_df.drop_duplicates("hash")
    ampliado_df = ampliado_df.head(1000).reset_index(drop=True)

    txs = ampliado_df.to_dict("records")
    n = len(txs)

    bloque_idx = set()
    direcciones_ocupadas = set()
    gas_usado = 0

    # --- TRIOS ---
    trios = []
    for i, j, k in combinations(range(n), 3):
        ti, tj, tk = txs[i], txs[j], txs[k]
        gi = ti.get("gas", 0) or 0
        gj = tj.get("gas", 0) or 0
        gk = tk.get("gas", 0) or 0
        gas_total = gi + gj + gk
        if gas_total > gas_limit:
            continue

        try:
            uij = calcular_utilidad(ti, tj, gas_limit=gas_limit)
            uik = calcular_utilidad(ti, tk, gas_limit=gas_limit)
            ujk = calcular_utilidad(tj, tk, gas_limit=gas_limit)
            utilidad = (uij + uik + ujk) / 3.0
        except Exception:
            utilidad = 0

        addrs = set()
        for t in (ti, tj, tk):
            if "from" in t: addrs.add(t["from"])
            if "to"   in t: addrs.add(t["to"])

        trios.append({
            "idx": [i, j, k],
            "utilidad": utilidad,
            "gas_total": gas_total,
            "addrs": addrs
        })
        if len(trios) >= max_trios:
            break
    trios.sort(key=lambda x: x["utilidad"], reverse=True)

    for t in trios:
        if t["addrs"] & direcciones_ocupadas:
            continue
        if gas_usado + t["gas_total"] > gas_limit:
            continue
        bloque_idx.update(t["idx"])
        gas_usado += t["gas_total"]
        direcciones_ocupadas |= t["addrs"]

    # --- PARES ---
    pares = []
    for i, j in combinations(range(n), 2):
        if i in bloque_idx or j in bloque_idx:
            continue
        ti, tj = txs[i], txs[j]
        gi = ti.get("gas", 0) or 0
        gj = tj.get("gas", 0) or 0
        gas_total = gi + gj
        if gas_total > gas_limit:
            continue

        try:
            utilidad = calcular_utilidad(ti, tj, gas_limit=gas_limit)
        except Exception:
            utilidad = 0

        addrs = set()
        for t in (ti, tj):
            if "from" in t: addrs.add(t["from"])
            if "to"   in t: addrs.add(t["to"])

        pares.append({
            "idx": [i, j],
            "utilidad": utilidad,
            "gas_total": gas_total,
            "addrs": addrs
        })
        if len(pares) >= max_pares:
            break
    pares.sort(key=lambda x: x["utilidad"], reverse=True)

    for p in pares:
        if any(idx in bloque_idx for idx in p["idx"]):
            continue
        if p["addrs"] & direcciones_ocupadas:
            continue
        if gas_usado + p["gas_total"] > gas_limit:
            continue
        bloque_idx.update(p["idx"])
        gas_usado += p["gas_total"]
        direcciones_ocupadas |= p["addrs"]

    # --- GREEDY de relleno ---
    hash_incluidas = set()
    if "hash" in ampliado_df.columns:
        hash_incluidas = {ampliado_df.loc[i, "hash"] for i in bloque_idx if i < len(ampliado_df)}

    if "hash" in ampliado_df.columns:
        txs_restantes = ampliado_df[~ampliado_df["hash"].isin(hash_incluidas)].copy()
    else:
        # sin hash, tomamos los que no están por índice (fallback)
        txs_restantes = ampliado_df.drop(index=list(bloque_idx), errors="ignore").copy()

    # Orden por “densidad” segura: usar gas_fee_cap (equivale a fee/gas si gas>0)
    txs_restantes["densidad"] = pd.to_numeric(txs_restantes.get("gas_fee_cap", 0), errors="coerce").fillna(0)
    txs_restantes = txs_restantes.sort_values("densidad", ascending=False)

    for _, row in txs_restantes.iterrows():
        g = row.get("gas", 0) or 0
        if gas_usado + g > gas_limit:
            continue
        idxs = ampliado_df.index[ (ampliado_df.get("hash", pd.Series(index=ampliado_df.index)) == row.get("hash")) ]
        idx = int(idxs[0]) if len(idxs) else int(row.name)  # fallback seguro
        if idx in bloque_idx:
            continue
        bloque_idx.add(idx)
        gas_usado += g

    # --- Finalizar ---
    bloque_df = ampliado_df.loc[list(bloque_idx)].copy()

    # lead time robusto
    bloque_df["lead_time_ms"] = pd.to_numeric(T_simulado, errors="coerce") - pd.to_numeric(bloque_df.get("timestamp_ms", 0), errors="coerce").fillna(0)

    gas_sum = _series_sum_int(bloque_df.get("gas"))
    fee_sum = _series_sum_int(bloque_df.get("fee"))
    real_util = _series_sum_int(pd.to_numeric(bloque_df.get("gas", 0), errors="coerce") *
                                pd.to_numeric(bloque_df.get("gas_fee_cap", 0), errors="coerce"))
    frag = gas_limit - gas_sum
    if frag < 0:  # por si alguna fila trae gas raro
        frag = 0

    lead_time_prom = bloque_df["lead_time_ms"]
    lead_time_prom = 0.0 if lead_time_prom.empty else float(pd.to_numeric(lead_time_prom, errors="coerce").dropna().mean() or 0.0)

    fin = time.perf_counter()

    resumen = {
        "algoritmo": "algoritmo_extendido_greedy",
        "timestamp_simulado": _safe_int(T_simulado),
        "total_transacciones": int(len(df)),
        "tx_incluidas": int(len(bloque_df)),
        "gas_usado": gas_sum,
        "utilidad_total_heuristica": fee_sum,
        "utilidad_total_real": real_util,
        "fragmentacion": int(frag),
        "lead_time_promedio_s": round(lead_time_prom / 1000.0, 3),
        "tiempo_ejecucion_s": round(fin - inicio, 4),
    }

    guardar_log_csv(resumen)
    return resumen, bloque_df
