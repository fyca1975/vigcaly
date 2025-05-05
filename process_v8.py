#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Procesador de archivos CSV para datos Calypso.

Este script automatiza el procesamiento, comparación y actualización de archivos CSV
con formatos de fecha específicos. Lee un archivo principal y lo compara con varios
archivos secundarios para actualizar valores según reglas definidas.
"""

import os
import shutil
import logging
import re
import argparse
from datetime import datetime
import pandas as pd
import numpy as np

# Configuración del sistema de logging
def setup_logging():
    """Configura el sistema de logging para el script."""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/procesamiento_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    # Crear logger específico para registros no encontrados
    no_encontrados_logger = logging.getLogger('no_encontrados')
    no_encontrados_logger.setLevel(logging.INFO)
    handler = logging.FileHandler('logs/log_no_encontrados.txt')
    handler.setFormatter(logging.Formatter('%(message)s'))
    no_encontrados_logger.addHandler(handler)
    
    return no_encontrados_logger

def crear_directorios():
    """Crea la estructura de directorios necesaria si no existe."""
    directorios = ['data', 'BK', 'procesados', 'logs']
    for directorio in directorios:
        if not os.path.exists(directorio):
            os.makedirs(directorio)
            logging.info(f"Directorio {directorio} creado")

def respaldar_archivos():
    """Copia todos los archivos de data/ a BK/ como respaldo."""
    try:
        archivos = os.listdir('data')
        for archivo in archivos:
            ruta_origen = os.path.join('data', archivo)
            ruta_destino = os.path.join('BK', archivo)
            
            if os.path.isfile(ruta_origen):
                shutil.copy2(ruta_origen, ruta_destino)
                logging.info(f"Archivo {archivo} respaldado en BK/")
        
        return True
    except Exception as e:
        logging.error(f"Error al respaldar archivos: {str(e)}")
        return False

def convertir_formato_fecha(fecha, formato_origen):
    """
    Convierte entre formatos de fecha DAAMMDD y AAAAMMDD.
    
    Args:
        fecha (str): Fecha en formato original
        formato_origen (str): 'corto' para DAAMMDD, 'largo' para AAAAMMDD
    
    Returns:
        str: Fecha convertida al formato alternativo
    """
    if formato_origen == 'corto':  # DAAMMDD a AAAAMMDD
        match = re.search(r'D(\d{6})$', fecha)
        if match:
            fecha_corta = match.group(1)
            ano_corto = fecha_corta[:2]
            mes = fecha_corta[2:4]
            dia = fecha_corta[4:6]
            ano_largo = '20' + ano_corto  # Asumimos que es del siglo 21
            return f"{ano_largo}{mes}{dia}"
    elif formato_origen == 'largo':  # AAAAMMDD a DAAMMDD
        match = re.search(r'(\d{8})$', fecha)
        if match:
            fecha_larga = match.group(1)
            ano_largo = fecha_larga[:4]
            mes = fecha_larga[4:6]
            dia = fecha_larga[6:8]
            ano_corto = ano_largo[2:4]
            return f"D{ano_corto}{mes}{dia}"
    
    logging.error(f"No se pudo convertir el formato de fecha: {fecha}")
    return None

def obtener_fecha_del_archivo(archivo_principal):
    """
    Extrae la fecha del nombre del archivo principal.
    
    Args:
        archivo_principal (str): Nombre del archivo principal
    
    Returns:
        tuple: (fecha_corta, fecha_larga)
    """
    match = re.search(r'VIG_TRANSACI_CALYPSO_D(\d{6})\.csv', archivo_principal)
    if match:
        fecha_corta = 'D' + match.group(1)
        fecha_larga = convertir_formato_fecha(fecha_corta, 'corto')
        return fecha_corta, fecha_larga
    
    logging.error(f"No se pudo extraer la fecha del archivo: {archivo_principal}")
    return None, None

def leer_archivo_csv(ruta_archivo):
    """
    Lee un archivo CSV ignorando la primera fila.
    
    Args:
        ruta_archivo (str): Ruta al archivo CSV
    
    Returns:
        pandas.DataFrame: DataFrame con los datos del archivo
    """
    try:
        # Leemos todo el archivo con pandas
        df = pd.read_csv(ruta_archivo, sep=';', header=None, dtype=str)
        # Ignoramos la primera fila como lo requiere la especificación
        if len(df) > 1:
            df_datos = df.iloc[1:].reset_index(drop=True)
            return df_datos
        else:
            logging.warning(f"El archivo {ruta_archivo} no tiene suficientes filas")
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error al leer el archivo {ruta_archivo}: {str(e)}")
        return pd.DataFrame()

def normalizar_valores(valor):
    """
    Normaliza valores numéricos eliminando ceros a la izquierda.
    
    Args:
        valor: Valor a normalizar
    
    Returns:
        str: Valor normalizado
    """
    if pd.isna(valor):
        return ""
    
    valor_str = str(valor).strip()
    
    # Si es un número, eliminar ceros a la izquierda
    if re.match(r'^\d+$', valor_str):
        return str(int(valor_str))
    
    return valor_str

def procesar_archivo_principal(fecha_corta, fecha_larga, no_encontrados_logger):
    """
    Procesa el archivo principal comparándolo con los archivos secundarios.
    
    Args:
        fecha_corta (str): Fecha en formato DAAMMDD
        fecha_larga (str): Fecha en formato AAAAMMDD
        no_encontrados_logger: Logger específico para registros no encontrados
    
    Returns:
        bool: True si el procesamiento fue exitoso, False en caso contrario
    """
    try:
        # Definimos nombres de archivos
        archivo_principal = f"VIG_TRANSACI_CALYPSO_{fecha_corta}.csv"
        archivo_fwd_div = f"fwd_div_Calypso_{fecha_larga}.csv"
        archivo_fwd_usd = f"fwd_usd_Calypso_{fecha_larga}.csv"
        archivo_liquidaciones = f"LIQUIDACIONES_{fecha_larga}.csv"
        
        # Verificamos existencia de archivos
        if not os.path.exists(os.path.join('data', archivo_principal)):
            logging.error(f"El archivo principal {archivo_principal} no existe")
            return False
        
        # Leemos el archivo principal
        ruta_principal = os.path.join('data', archivo_principal)
        df_principal = leer_archivo_csv(ruta_principal)
        
        if df_principal.empty:
            logging.error(f"No se pudo leer el archivo principal o está vacío")
            return False
        
        # Leemos los archivos secundarios si existen
        df_fwd_div = pd.DataFrame()
        df_fwd_usd = pd.DataFrame()
        df_liquidaciones = pd.DataFrame()
        
        if os.path.exists(os.path.join('data', archivo_fwd_div)):
            df_fwd_div = leer_archivo_csv(os.path.join('data', archivo_fwd_div))
        else:
            logging.warning(f"El archivo {archivo_fwd_div} no existe")
        
        if os.path.exists(os.path.join('data', archivo_fwd_usd)):
            df_fwd_usd = leer_archivo_csv(os.path.join('data', archivo_fwd_usd))
        else:
            logging.warning(f"El archivo {archivo_fwd_usd} no existe")
        
        if os.path.exists(os.path.join('data', archivo_liquidaciones)):
            df_liquidaciones = leer_archivo_csv(os.path.join('data', archivo_liquidaciones))
        else:
            logging.warning(f"El archivo {archivo_liquidaciones} no existe")
        
        # Inicializar contadores para el resumen
        total_filas = len(df_principal)
        modificadas = 0
        no_encontradas = 0
        
        # Procesamos cada fila del archivo principal
        for i, fila in df_principal.iterrows():
            valor_buscar = normalizar_valores(fila[4]) if len(fila) > 4 else ""
            valor_actual = fila[3] if len(fila) > 3 else ""
            
            # Variable para controlar si se encontró coincidencia
            coincidencia_encontrada = False
            nuevo_valor = "no encontrado"  # Valor por defecto si no hay coincidencia
            
            # Buscamos en fwd_div
            if not df_fwd_div.empty:
                for j, fila_div in df_fwd_div.iterrows():
                    valor_comparar = normalizar_valores(fila_div[43]) if len(fila_div) > 43 else ""
                    
                    if valor_buscar == valor_comparar:
                        nuevo_valor = fila_div[0] if len(fila_div) > 0 else "no encontrado"
                        coincidencia_encontrada = True
                        logging.info(f"Coincidencia en fwd_div - Fila {i+2}: {valor_buscar} -> {nuevo_valor}")
                        break
            
            # Si no se encontró en fwd_div, buscamos en fwd_usd
            if not coincidencia_encontrada and not df_fwd_usd.empty:
                for j, fila_usd in df_fwd_usd.iterrows():
                    valor_comparar = normalizar_valores(fila_usd[43]) if len(fila_usd) > 43 else ""
                    
                    if valor_buscar == valor_comparar:
                        nuevo_valor = fila_usd[0] if len(fila_usd) > 0 else "no encontrado"
                        coincidencia_encontrada = True
                        logging.info(f"Coincidencia en fwd_usd - Fila {i+2}: {valor_buscar} -> {nuevo_valor}")
                        break
            
            # Si no se encontró en los anteriores, buscamos en liquidaciones
            if not coincidencia_encontrada and not df_liquidaciones.empty:
                for j, fila_liq in df_liquidaciones.iterrows():
                    valor_comparar = normalizar_valores(fila_liq[1]) if len(fila_liq) > 1 else ""
                    
                    if valor_buscar == valor_comparar:
                        nuevo_valor = fila_liq[20] if len(fila_liq) > 20 else "no encontrado"
                        coincidencia_encontrada = True
                        logging.info(f"Coincidencia en liquidaciones - Fila {i+2}: {valor_buscar} -> {nuevo_valor}")
                        break
            
            # Actualizamos el valor en el DataFrame principal
            if coincidencia_encontrada:
                modificadas += 1
            else:
                no_encontradas += 1
                no_encontrados_logger.info(f"Fila {i+2}: Valor no encontrado: {valor_buscar}")
            
            df_principal.at[i, 3] = nuevo_valor
        
        # Guardamos el archivo procesado
        if not os.path.exists('procesados'):
            os.makedirs('procesados')
        
        ruta_salida = os.path.join('procesados', archivo_principal)
        
        # Leer el archivo original para preservar la primera fila
        df_original = pd.read_csv(ruta_principal, sep=';', header=None, dtype=str)
        primera_fila = df_original.iloc[0:1]
        
        # Concatenar la primera fila con los datos procesados
        df_resultado = pd.concat([primera_fila, df_principal], ignore_index=True)
        
        # Guardar el resultado
        df_resultado.to_csv(ruta_salida, sep=';', index=False, header=False)
        logging.info(f"Archivo procesado guardado en {ruta_salida}")
        
        # Mostrar resumen
        logging.info("----- RESUMEN DE PROCESAMIENTO -----")
        logging.info(f"Total de registros procesados: {total_filas}")
        logging.info(f"Registros modificados: {modificadas}")
        logging.info(f"Registros no encontrados: {no_encontradas}")
        logging.info("-----------------------------------")
        
        return True
    
    except Exception as e:
        logging.error(f"Error en el procesamiento del archivo principal: {str(e)}")
        return False

def procesar_todos_los_archivos():
    """
    Procesa todos los archivos VIG_TRANSACI_CALYPSO en la carpeta data/.
    
    Returns:
        int: Número de archivos procesados exitosamente
    """
    archivos_procesados = 0
    try:
        archivos = os.listdir('data')
        archivos_principales = [a for a in archivos if re.match(r'VIG_TRANSACI_CALYPSO_D\d{6}\.csv', a)]
        
        if not archivos_principales:
            logging.warning("No se encontraron archivos principales para procesar")
            return 0
        
        # Configuramos un logger específico para registros no encontrados
        no_encontrados_logger = logging.getLogger('no_encontrados')
        
        for archivo in archivos_principales:
            fecha_corta, fecha_larga = obtener_fecha_del_archivo(archivo)
            if fecha_corta and fecha_larga:
                logging.info(f"Procesando archivo: {archivo}")
                if procesar_archivo_principal(fecha_corta, fecha_larga, no_encontrados_logger):
                    archivos_procesados += 1
        
        return archivos_procesados
    
    except Exception as e:
        logging.error(f"Error al procesar todos los archivos: {str(e)}")
        return archivos_procesados

def main():
    """Función principal que coordina la ejecución del script."""
    parser = argparse.ArgumentParser(description='Procesador de archivos CSV Calypso')
    parser.add_argument('--fecha', type=str, help='Fecha específica en formato DAAMMDD para procesar')
    parser.add_argument('--todos', action='store_true', help='Procesar todos los archivos disponibles')
    args = parser.parse_args()
    
    # Configurar logging
    no_encontrados_logger = setup_logging()
    logging.info("Iniciando procesamiento de archivos CSV")
    
    # Crear estructura de directorios
    crear_directorios()
    
    # Hacer respaldo de archivos
    if not respaldar_archivos():
        logging.error("No se pudo realizar el respaldo de archivos. Abortando.")
        return
    
    if args.fecha:
        # Procesar un archivo específico por fecha
        fecha_corta = f"D{args.fecha}"
        fecha_larga = convertir_formato_fecha(fecha_corta, 'corto')
        
        if fecha_larga:
            logging.info(f"Procesando archivo para la fecha: {args.fecha}")
            procesar_archivo_principal(fecha_corta, fecha_larga, no_encontrados_logger)
        else:
            logging.error(f"Formato de fecha inválido: {args.fecha}")
    
    elif args.todos:
        # Procesar todos los archivos
        logging.info("Procesando todos los archivos disponibles")
        archivos_procesados = procesar_todos_los_archivos()
        logging.info(f"Total de archivos procesados: {archivos_procesados}")
    
    else:
        # Si no se especificó ninguna opción, procesar el archivo más reciente
        try:
            archivos = os.listdir('data')
            archivos_principales = [a for a in archivos if re.match(r'VIG_TRANSACI_CALYPSO_D\d{6}\.csv', a)]
            
            if archivos_principales:
                # Ordenar por fecha (la fecha está en el nombre del archivo)
                ultimo_archivo = sorted(archivos_principales)[-1]
                fecha_corta, fecha_larga = obtener_fecha_del_archivo(ultimo_archivo)
                
                if fecha_corta and fecha_larga:
                    logging.info(f"Procesando el archivo más reciente: {ultimo_archivo}")
                    procesar_archivo_principal(fecha_corta, fecha_larga, no_encontrados_logger)
                else:
                    logging.error("No se pudo determinar la fecha del archivo más reciente")
            else:
                logging.warning("No se encontraron archivos para procesar")
        
        except Exception as e:
            logging.error(f"Error al procesar el archivo más reciente: {str(e)}")
    
    logging.info("Finalizado el procesamiento de archivos CSV")

if __name__ == "__main__":
    main()