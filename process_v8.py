def limpiar_archivo(ruta_archivo, ruta_limpia):
    """Crea una versión limpia del archivo, reemplazando caracteres problemáticos."""
    with open(ruta_archivo, 'rb') as file_in:
        contenido = file_in.read()
    
    # Reemplazar caracteres problemáticos
    contenido_limpio = contenido.replace(b'\xd1', b'N')  # Ejemplo: reemplazar ñ con N
    
    with open(ruta_limpia, 'wb') as file_out:
        file_out.write(contenido_limpio)