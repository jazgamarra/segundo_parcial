import pandas as pd
import os 

def cargar_dataset(path, nrows=1000):
    """
    Carga un subconjunto del dataset de mempool y selecciona únicamente
    las columnas necesarias para la simulación de construcción de bloques.

    Parámetros:
        path (str): Ruta al archivo .csv de mempool (formato Flashbots).
        nrows (int): Número de filas a cargar (default: 1000).

    Retorna:
        pd.DataFrame: Con columnas ['hash', 'from', 'to', 'gas', 'gas_fee_cap', 'timestamp_ms']
    """
    df = pd.read_csv(path, nrows=nrows)
    columnas_necesarias = ["hash", "from", "to", "gas", "gas_fee_cap", "timestamp_ms"]
    return df[columnas_necesarias].copy()

def guardar_log_csv(resumen, path="logs/logs.csv"):
    df_log = pd.DataFrame([resumen])

    if os.path.exists(path):
        df_log.to_csv(path, mode='a', header=False, index=False)
    else:
        df_log.to_csv(path, mode='w', header=True, index=False)

def calcular_T_simulado(df, delay_ms=6000):
    """
    Calcula el instante simulado de inclusión del bloque.

    Parámetros:
        df (pd.DataFrame): DataFrame con columna 'timestamp_ms' de las transacciones.
        delay_ms (int): Retardo simulado (en milisegundos) respecto al promedio de llegada.

    Retorna:
        int: Timestamp simulado en milisegundos.
    """
    return int(df["timestamp_ms"].mean()) + delay_ms

def calcular_utilidad(ti, tj, gas_limit=30_000_000, penalties=None, bonuses=None):
    penalties = penalties or {
        "conflicto": 999,
        "dependencia_mal_ordenada": 100,
        "gas_alto": 10
    }
    bonuses = bonuses or {
        "contrato_comun": 50,
        "orden_correcto": 30,
        "mev_detectado": 100
    }

    tarifa_ti = ti["gas"] * ti["gas_fee_cap"]
    tarifa_tj = tj["gas"] * tj["gas_fee_cap"]

    reglas = {
        "conflicto_nonce": ti["from"] == tj["from"] and ti.get("nonce") == tj.get("nonce"),
        "conflicto_destino": ti["to"] == tj["to"],
        "gas_excesivo": (ti["gas"] + tj["gas"]) > gas_limit,
        "contrato_comun": ti["to"] == tj["to"],
        "orden_valido": (
            ti["from"] == tj["from"]
            and ti.get("nonce") is not None
            and tj.get("nonce") is not None
            and ti["nonce"] + 1 == tj["nonce"]
        ),
    }

    penalizacion = 0
    bonificacion = 0

    if reglas["conflicto_nonce"] or reglas["conflicto_destino"]:
        penalizacion += penalties["conflicto"]
    if reglas["gas_excesivo"]:
        penalizacion += penalties["gas_alto"]
    if reglas["contrato_comun"]:
        bonificacion += bonuses["contrato_comun"]
    if reglas["orden_valido"]:
        bonificacion += bonuses["orden_correcto"]

    return tarifa_ti + tarifa_tj + bonificacion - penalizacion
