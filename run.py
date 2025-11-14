from utils import cargar_dataset, calcular_T_simulado
from algoritmo_base import construir_bloque
import os 

df = cargar_dataset(os.path.join(os.path.dirname(__file__), '..', 'data', '2025-07-14.csv'), nrows=1000)
T_simulado = calcular_T_simulado(df)
resumen, bloque = construir_bloque(df, T_simulado, top_n=500)

print(resumen)
