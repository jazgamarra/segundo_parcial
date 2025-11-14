

import json
import os
from pathlib import Path

from utils import cargar_dataset, guardar_log_csv
from algoritmo_greedy_clasico import construir_bloque

# -------- CONFIGURACIÓN --------
BLOCKS = [23506390, 23506393, 23506414]
TOP_N = 500
# -------------------------------

HERE = Path(__file__).resolve().parent           
DATASETS_DIR = HERE / "prepare_data_r2" / "datasets"
BLOCKS_DIR   = HERE / "data_release_2" / "blocks"
LOGS_DIR     = HERE / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)    

def leer_timestamp_ms_del_bloque(block_number: int) -> int:
    """Lee el timestamp (hex) del bloque real en milisegundos."""
    block_path = BLOCKS_DIR / f"block_{block_number}.json"
    if not block_path.exists():
        raise FileNotFoundError(f"No existe el JSON del bloque: {block_path}")
    with open(block_path, "r", encoding="utf-8") as f:
        blk = json.load(f)
    ts_hex = blk.get("timestamp")
    ts_ms = int(ts_hex, 16) * 1000 if isinstance(ts_hex, str) and ts_hex.startswith("0x") else int(ts_hex) * 1000
    return ts_ms

def correr_un_bloque(block_number: int):
    """Carga dataset, lee T_simulado real y ejecuta el algoritmo."""
    csv_path = DATASETS_DIR / str(block_number) / "pending_formatted.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"No existe dataset preparado: {csv_path}")

    df = cargar_dataset(str(csv_path), nrows=10**9)
    T_simulado = leer_timestamp_ms_del_bloque(block_number)

    resumen, bloque = construir_bloque(df, T_simulado)
    resumen["block_number"] = block_number
    print(f"\n=== Bloque {block_number} ===")
    print(resumen)
    guardar_log_csv(resumen, path=str(LOGS_DIR / "logs.csv"))

def main():
    for b in BLOCKS:
        try:
            correr_un_bloque(b)
        except Exception as e:
            print(f"[ERROR] Bloque {b}: {e}")

if __name__ == "__main__":
    main()
