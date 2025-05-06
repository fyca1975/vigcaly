import os
import shutil
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# Configurar logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, "procesamiento.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

no_encontrados_path = os.path.join(log_dir, "log_no_encontrados.txt")

# Rutas
DATA_DIR = "data"
BK_DIR = "BK"
PROCESADOS_DIR = "procesados"
os.makedirs(BK_DIR, exist_ok=True)
os.makedirs(PROCESADOS_DIR, exist_ok=True)


def respaldar_archivos():
    for archivo in os.listdir(DATA_DIR):
        origen = os.path.join(DATA_DIR, archivo)
        destino = os.path.join(BK_DIR, archivo)
        shutil.copy2(origen, destino)
    logging.info("Todos los archivos han sido respaldados correctamente.")


def cargar_csv_sin_encabezado(path, usecols=None):
    return pd.read_csv(path, sep=";", header=None, usecols=usecols, skiprows=1, dtype=str, encoding="latin1")

def normalizar_valor(valor):
    return str(valor).lstrip("0") if pd.notna(valor) else ""


def procesar_vig_transaci(file_name):
    fecha_aammdd = file_name.split("_")[-1].split(".")[0][-6:]
    fecha_aaaammdd = "20" + fecha_aammdd

    path_vig = os.path.join(DATA_DIR, file_name)
    df_vig = pd.read_csv(path_vig, sep=";", header=None, skiprows=1, dtype=str,  encoding="latin1")

    # Cargar archivos secundarios
    try:
        fwd_div = cargar_csv_sin_encabezado(os.path.join(DATA_DIR, f"fwd_div_Calypso_{fecha_aaaammdd}.csv"), usecols=[0, 43])
        fwd_usd = cargar_csv_sin_encabezado(os.path.join(DATA_DIR, f"fwd_usd_Calypso_{fecha_aaaammdd}.csv"), usecols=[0, 34])
        liqui = cargar_csv_sin_encabezado(os.path.join(DATA_DIR, f"LIQUIDACIONES_{fecha_aaaammdd}.csv"), usecols=[0, 17])
    except FileNotFoundError as e:
        logging.error(f"Archivo secundario faltante: {e}")
        return

    no_encontrados = []

    for idx, row in df_vig.iterrows():
        clave = normalizar_valor(row[4])
        nuevo_valor = "no encontrado"

        # Buscar en fwd_div
        match = fwd_div[fwd_div[43].apply(normalizar_valor) == clave]
        if not match.empty:
            nuevo_valor = match.iloc[0, 0]
            origen = "fwd_div"

        else:
            match = fwd_usd[fwd_usd[34].apply(normalizar_valor) == clave]
            if not match.empty:
                nuevo_valor = match.iloc[0, 0]
                origen = "fwd_usd"

            else:
                match = liqui[liqui[0].apply(normalizar_valor) == clave]
                if not match.empty:
                    nuevo_valor = liqui.iloc[0, 1]
                    origen = "LIQUIDACIONES"
                else:
                    origen = "ninguno"
                    no_encontrados.append(f"Fila {idx+2}: valor {clave} no encontrado.")

        df_vig.at[idx, 3] = nuevo_valor
        logging.info(f"Fila {idx+2}: valor columna 4 actualizado con '{nuevo_valor}' desde {origen}.")

    # Guardar archivo procesado
    output_path = os.path.join(PROCESADOS_DIR, file_name)
    df_vig.to_csv(output_path, sep=";", index=False, header=False)

    # Guardar log de no encontrados
    with open(no_encontrados_path, "a", encoding="utf-8") as f:
        for linea in no_encontrados:
            f.write(linea + "\n")

    logging.info(f"Archivo procesado y guardado: {output_path}")


if __name__ == "__main__":
    respaldar_archivos()
    for archivo in os.listdir(DATA_DIR):
        if archivo.startswith("VIG_TRANSACI_CALYPSO_D") and archivo.endswith(".csv"):
            procesar_vig_transaci(archivo)
