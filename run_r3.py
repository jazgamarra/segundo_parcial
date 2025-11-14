import re
from pathlib import Path

from utils import cargar_dataset, guardar_log_csv
from algoritmo_extendido_greedy import construir_bloque  # o cambia al que quieras

# -------- CONFIG --------
TOP_N = 500
DATASETS_SUBDIR = "release3/datasets"
LOGFILE = "release3/logs_r3.csv"
# ------------------------

HERE = Path(__file__).resolve().parent
DATASETS_DIR = HERE / DATASETS_SUBDIR
LOGS_PATH = HERE / LOGFILE
LOGS_PATH.parent.mkdir(parents=True, exist_ok=True)

CSV_PATTERN = re.compile(r"mempool_datos_bloque_(\d+)\.csv$", re.IGNORECASE)

def listar_csv_mempool():
    if not DATASETS_DIR.exists():
        raise FileNotFoundError(f"No existe la carpeta de datasets: {DATASETS_DIR}")
    files = []
    for p in DATASETS_DIR.iterdir():
        if p.is_file() and CSV_PATTERN.search(p.name):
            files.append(p)
    return sorted(files)

def inferir_T_simulado(df):
    """
    T_simulado = referencia de tiempo para construir el bloque.
    No usamos JSON. Lo inferimos del CSV.
    Estrategia:
      1) Si existe 'timestamp_ms' (arribo/observación por tx), usamos max() de esa columna.
      2) Si no, intentamos otras columnas habituales (por si tu CSV trae otro nombre).
    """
    candidatos = [
        "timestamp_ms",                 # esperado en tus CSV de mempool
        "snapshot_ts_ms",
        "snapshot_timestamp_ms",
        "captured_at_ms",
        "included_block_timestamp_ms"   # por si viene de otro pipeline
    ]
    for col in candidatos:
        if col in df.columns:
            try:
                val = int(df[col].max())
                if val > 0:
                    return val
            except Exception:
                pass
    raise ValueError(
        "No pude inferir T_simulado desde el CSV. "
        "Necesito al menos una columna de tiempo, idealmente 'timestamp_ms'."
    )

def correr_csv(csv_path: Path):
    # Extraer block_number del nombre del archivo (si está)
    m = CSV_PATTERN.search(csv_path.name)
    block_number = int(m.group(1)) if m else None

    # Cargar dataset completo
    df = cargar_dataset(str(csv_path), nrows=10**9)

    # Inferir T_simulado sin JSON
    T_simulado = inferir_T_simulado(df)

    # Ejecutar heurística
    resumen, bloque = construir_bloque(df, T_simulado)

    # Completar/estandarizar el resumen y loguear
    if block_number is not None:
        resumen["block_number"] = block_number
    resumen.setdefault("dataset_file", csv_path.name)
    resumen.setdefault("num_tx_input", len(df))
    resumen.setdefault("top_n", TOP_N)

    print(f"\n=== Dataset: {csv_path.name} ===")
    print(resumen)
    guardar_log_csv(resumen, path=str(LOGS_PATH))

def main():
    csvs = listar_csv_mempool()
    if not csvs:
        print(f"No encontré CSVs de mempool en {DATASETS_DIR}")
        return

    for csv_path in csvs:
        try:
            correr_csv(csv_path)
        except Exception as e:
            print(f"[ERROR] {csv_path.name}: {e}")

if __name__ == "__main__":
    main()
