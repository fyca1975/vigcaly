import os
import shutil
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from glob import glob

# Configurar logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/log_script.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger()

# Log para no encontrados
not_found_log_path = "logs/log_no_encontrados.txt"

# Carpetas
DATA_DIR = "data"
BK_DIR = "BK"
PROCESADOS_DIR = "procesados"

# Crear carpetas si no existen
os.makedirs(BK_DIR, exist_ok=True)
os.makedirs(PROCESADOS_DIR, exist_ok=True)


def respaldar_archivos():
    for file in glob(f"{DATA_DIR}/*.csv"):
        shutil.copy(file, BK_DIR)
    logger.info("Archivos respaldados correctamente.")


def leer_csv(path):
    try:
        return pd.read_csv(path, sep=';', skiprows=1, header=None, encoding='latin1')
    except Exception as e:
        logger.error(f"Error al leer archivo {path}: {e}")
        return None


def normalizar(valor):
    return str(int(valor)) if pd.notna(valor) and str(valor).isdigit() else str(valor)


def procesar_archivo_vig(vig_path):
    # Extraer fechas del nombre
    filename = os.path.basename(vig_path)
    fecha_daammdd = filename.split("_")[-1].replace(".csv", "")
    fecha_aaaammdd = "20" + fecha_daammdd  # asume siglo 21

    # Construir rutas
    path_div = os.path.join(DATA_DIR, f"fwd_div_Calypso_{fecha_aaaammdd}.csv")
    path_usd = os.path.join(DATA_DIR, f"fwd_usd_Calypso_{fecha_aaaammdd}.csv")
    path_liq = os.path.join(DATA_DIR, f"LIQUIDACIONES_{fecha_aaaammdd}.csv")

    # Leer archivos
    vig_df = leer_csv(vig_path)
    div_df = leer_csv(path_div)
    usd_df = leer_csv(path_usd)
    liq_df = leer_csv(path_liq)

    if vig_df is None:
        logger.error("No se pudo procesar el archivo VIG.")
        return

    encontrados = modificados = no_encontrados = 0

    with open(not_found_log_path, "a", encoding="utf-8") as nf:
        for idx, row in vig_df.iterrows():
            if len(row) < 5:
                continue

            val_comparar = normalizar(row[4])
            nuevo_valor = None

            # Buscar en fwd_div
            if div_df is not None:
                match = div_df[div_df[44].apply(normalizar) == val_comparar]
                if not match.empty:
                    nuevo_valor = match.iloc[0, 0]

            # Buscar en fwd_usd
            if nuevo_valor is None and usd_df is not None:
                match = usd_df[usd_df[44].apply(normalizar) == val_comparar]
                if not match.empty:
                    nuevo_valor = match.iloc[0, 0]

            # Buscar en LIQUIDACIONES
            if nuevo_valor is None and liq_df is not None:
                match = liq_df[liq_df[1].apply(normalizar) == val_comparar]
                if not match.empty and liq_df.shape[1] >= 21:
                    nuevo_valor = match.iloc[0, 20]

            if nuevo_valor:
                vig_df.at[idx, 3] = nuevo_valor
                logger.info(f"Fila {idx}: reemplazado valor columna 4 por {nuevo_valor}")
                modificados += 1
            else:
                vig_df.at[idx, 3] = "no encontrado"
                nf.write(f"No encontrado: fila {idx}, valor {val_comparar}\n")
                logger.warning(f"No encontrado: fila {idx}, valor {val_comparar}")
                no_encontrados += 1

            encontrados += 1

    output_path = os.path.join(PROCESADOS_DIR, filename)
    try:
        vig_df.to_csv(output_path, sep=';', header=False, index=False, encoding='latin1')
        logger.info(f"Archivo procesado guardado en: {output_path}")
    except Exception as e:
        logger.error(f"Error al guardar archivo procesado: {e}")

    logger.info(f"Resumen archivo {filename}: Total={encontrados}, Modificados={modificados}, No encontrados={no_encontrados}")


def procesar_todos():
    respaldar_archivos()
    archivos = glob(f"{DATA_DIR}/VIG_TRANSACI_CALYPSO_*.csv")
    if not archivos:
        logger.warning("No se encontraron archivos VIG_TRANSACI_CALYPSO en data/")
    for path in archivos:
        procesar_archivo_vig(path)


if __name__ == "__main__":
    procesar_todos()
