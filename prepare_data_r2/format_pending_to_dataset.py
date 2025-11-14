# prepare_data_r2/format_pending_to_dataset.py
# -*- coding: utf-8 -*-
import os, json, time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd

# ==========================
# 1) CONFIG SIMPLE (EDITAR)
# ==========================
ALCHEMY_HTTP = "https://eth-mainnet.g.alchemy.com/v2/Mb0w1SreNP0tXz9xGTK9f"  # <- tu key (HTTP)
# Si querés limitar a ciertos bloques, ponlos aquí; si la lista está vacía, procesa TODOS los snapshots:
BLOCKS = []  # ejemplo: [23506390, 23506393, 23506414]
MAX_WORKERS = 16
HTTP_TIMEOUT = 20


ROOT = Path(__file__).resolve().parents[0]                 # prepare_data_r2/
SNAP_DIR = ROOT.parent / "data_release_2" / "snapshots"    # data_release_2/snapshots
OUT_ROOT = ROOT / "datasets"                               # prepare_data_r2/datasets

HEADER = [
    "timestamp_ms","hash","chain_id","from","to","value","nonce","gas",
    "gas_price","gas_tip_cap","gas_fee_cap","data_size","data_4bytes",
    "sources","included_at_block_height","included_block_timestamp_ms",
    "inclusion_delay_ms","tx_type"
]

# ==========================
# 2) UTILES
# ==========================
session = requests.Session()
session.headers.update({"Content-Type": "application/json", "User-Agent": "prepare_data_r2/1.0"})

def jrpc(method, params):
    payload = {"jsonrpc":"2.0","id":1,"method":method,"params":params}
    r = session.post(ALCHEMY_HTTP, json=payload, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    j = r.json()
    if "error" in j:
        raise RuntimeError(str(j["error"]))
    return j["result"]

def to_int(x):
    if x is None: return 0
    if isinstance(x, str) and x.startswith("0x"):
        return int(x, 16)
    try:
        return int(x)
    except Exception:
        return 0

def iso_to_ms(iso_str: str | None) -> int:
    if not iso_str: 
        return int(time.time()*1000)
    return int(datetime.fromisoformat(iso_str.replace("Z","+00:00")).timestamp() * 1000)

# ==========================
# 3) PROCESADO
# ==========================
def process_block_snapshot(snap_path: Path):
    with open(snap_path, "r", encoding="utf-8") as f:
        snap = json.load(f)

    meta = snap.get("meta", {})
    target_block = int(meta.get("target_block"))
    hashes = snap.get("pending_hashes", [])
    ts_ms = iso_to_ms(meta.get("captured_at"))

    if not hashes:
        print(f"[skip] {snap_path.name}: 0 pending")
        return

    # Enriquecer transacciones por RPC
    def fetch_tx(h):
        try:
            return jrpc("eth_getTransactionByHash", [h])
        except Exception:
            return None

    results = []
    misses = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_tx, h): h for h in hashes}
        for fut in as_completed(futs):
            tx = fut.result()
            if tx: results.append(tx)
            else:  misses += 1

    # Armar filas con CABECERA FIJA
    rows = []
    for tx in results:
        inp = tx.get("input") or "0x"
        data_size = max(len(inp)//2 - 1, 0) if inp.startswith("0x") else len(inp)
        data_4b = (inp[:10] if inp.startswith("0x") and len(inp) >= 10 else "")

        row = {
            "timestamp_ms": ts_ms,
            "hash": tx.get("hash"),
            "chain_id": to_int(tx.get("chainId")) or 1,
            "from": tx.get("from"),
            "to": tx.get("to"),
            "value": to_int(tx.get("value")),
            "nonce": to_int(tx.get("nonce")),
            "gas": to_int(tx.get("gas")),
            "gas_price": to_int(tx.get("gasPrice")),                      
            "gas_tip_cap": to_int(tx.get("maxPriorityFeePerGas")),         
            "gas_fee_cap": (to_int(tx.get("maxFeePerGas")) or to_int(tx.get("gasPrice"))),
            "data_size": data_size,
            "data_4bytes": data_4b,
            "sources": "alchemy_ws",                                   
            "included_at_block_height": 0,                             
            "included_block_timestamp_ms": 0,
            "inclusion_delay_ms": 0,
            "tx_type": tx.get("type") or ""
        }

        # asegurar orden exacto
        rows.append([row.get(col, "") for col in HEADER])

    out_dir = OUT_ROOT / str(target_block)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "pending_formatted.csv"
    pd.DataFrame(rows, columns=HEADER).to_csv(out_csv, index=False)

    meta_out = {
        "snapshot_file": snap_path.name,
        "target_block": target_block,
        "captured_at": meta.get("captured_at"),
        "n_pending_hashes_raw": len(hashes),
        "n_enriched": len(results),
        "misses": misses
    }
    with open(out_dir / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta_out, f, ensure_ascii=False, indent=2)

    print(f"[ok] {snap_path.name} → {out_csv}  (enriched={len(results)}, misses={misses})")

def main():
    if BLOCKS:
        snaps = [SNAP_DIR / f"snap_pending_target{b}.json" for b in BLOCKS]
    else:
        snaps = sorted(SNAP_DIR.glob("snap_pending_target*.json"))

    if not snaps:
        print(f"[err] No hay snapshots en {SNAP_DIR}")
        return

    for p in snaps:
        if p.exists():
            process_block_snapshot(p)
        else:
            print(f"[warn] No existe {p}")

if __name__ == "__main__":
    main()
