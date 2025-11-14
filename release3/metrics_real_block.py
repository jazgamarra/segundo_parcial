import json
from pathlib import Path

def parse_int(x):
    """Convierte string/int/hex a entero seguro."""
    if x is None:
        return 0
    if isinstance(x, int):
        return x
    if isinstance(x, str):
        try:
            return int(x, 16) if x.startswith("0x") else int(x)
        except Exception:
            return 0
    return 0


def calcular_metricas_bloque_real(block_json_path):
    """Calcula métricas del bloque real: gas, utilidad, fragmentación, etc."""
    with open(block_json_path, "r", encoding="utf-8") as f:
        block = json.load(f)

    block_number = parse_int(block.get("number"))
    gas_used = parse_int(block.get("gasUsed"))
    gas_limit = parse_int(block.get("gasLimit", 30_000_000))
    base_fee = parse_int(block.get("baseFeePerGas"))
    total_tx = len(block.get("transactions", []))

    # Utilidad estimada: gasUsed * baseFeePerGas
    utilidad_total = gas_used * base_fee
    fragmentacion = max(gas_limit - gas_used, 0)

    return {
        "bloque": block_number,
        "total_transacciones": total_tx,
        "gas_usado": gas_used,
        "gas_limit": gas_limit,
        "utilidad_total_real": utilidad_total,
        "fragmentacion": fragmentacion,
        "base_fee_per_gas": base_fee,
        "tiempo_ejecucion_s": 0.0
    }


if __name__ == "__main__":
    HERE = Path(__file__).resolve().parent
    # Cambiá el nombre del archivo si querés procesar otro bloque
    path = HERE / "datasets" / "bloque_23748341.json"

    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo JSON en: {path}")

    result = calcular_metricas_bloque_real(path)

    print("\n=== MÉTRICAS BLOQUE REAL ===")
    for k, v in result.items():
        print(f"{k}: {v:,}")
