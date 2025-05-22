# -*- coding: utf-8 -*-
"""
Created on Wed May 21 14:42:25 2025

@author: alopagui
"""

import pandas as pd

def realizar_barrido_circuito(df_circuitos, df_elementos_corte, df_lineas):
    """
    Realiza un barrido iterativo para determinar la conexión eléctrica entre elementos de un sistema de distribución.

    Args:
        - df_circuitos (pd.DataFrame): DataFrame con la lista de circuitos.
        - df_elementos_corte (pd.DataFrame): DataFrame con la información de elementos de corte y transformadores.
        - df_lineas (pd.DataFrame): DataFrame con la información de las líneas de distribución.

    Returns:
        tuple: Una tupla que contiene dos DataFrames:
               - df_elementos_corte_resultado: Elementos de corte con su padre y elementos aguas arriba.
               - df_lineas_resultado: Líneas con su padre (elemento de corte) y elementos de corte aguas arriba.
    """

    # DataFrames para almacenar los resultados
    df_elementos_corte_resultado = pd.DataFrame(columns=['G3E_FID', 'NODO1_ID', 'NODO2_ID', 'CIRCUITO', 'CODIGO_OPERATIVO', 'EQUIPO_PADRE', 'ELEMENTOS_AGUAS_ARRIBA'])
    df_lineas_resultado = pd.DataFrame(columns=['G3E_FID', 'NODO1_ID', 'NODO2_ID', 'CIRCUITO', 'EQUIPO_PADRE', 'ELEMENTOS_AGUAS_ARRIBA_CORTE'])

    # Iterar sobre cada circuito en el archivo de circuitos
    for _, circuito_row in df_circuitos.iterrows():
        circuito_actual = circuito_row['Circuito']
        print(f"Iniciando barrido para el circuito: {circuito_actual}")

        # Filtrar el interruptor inicial del circuito
        interruptor_inicial = df_elementos_corte[
            (df_elementos_corte['CIRCUITO'] == circuito_actual) &
            (df_elementos_corte['CODIGO_OPERATIVO'] == circuito_actual)
        ].iloc[0]

        # Nodos de partida para el barrido
        nodo_actual = interruptor_inicial['NODO2_ID']
        codigo_operativo_padre = interruptor_inicial['CODIGO_OPERATIVO']
        elementos_aguas_arriba_corte = [codigo_operativo_padre]

        # Copias modificables de los DataFrames para el barrido
        df_elementos_corte_mod = df_elementos_corte.copy()
        df_lineas_mod = df_lineas.copy()

        # Eliminar el interruptor inicial de las copias para que no se vuelva a procesar
        df_elementos_corte_mod = df_elementos_corte_mod[
            (df_elementos_corte_mod['G3E_FID'] != interruptor_inicial['G3E_FID'])
        ].reset_index(drop=True)

        # Bucle principal del barrido iterativo
        while True:
            encontrado_en_lineas = False
            encontrado_en_cortes = False

            # 1. Buscar en las líneas
            lineas_conectadas = df_lineas_mod[df_lineas_mod['NODO1_ID'] == nodo_actual]

            if not lineas_conectadas.empty:
                for _, linea_row in lineas_conectadas.iterrows():
                    nueva_linea = {
                        'G3E_FID': linea_row['G3E_FID'],
                        'NODO1_ID': linea_row['NODO1_ID'],
                        'NODO2_ID': linea_row['NODO2_ID'],
                        'CIRCUITO': linea_row['CIRCUITO'],
                        'EQUIPO_PADRE': codigo_operativo_padre,
                        'ELEMENTOS_AGUAS_ARRIBA_CORTE': ','.join(elementos_aguas_arriba_corte)
                    }
                    df_lineas_resultado = pd.concat([df_lineas_resultado, pd.DataFrame([nueva_linea])], ignore_index=True)

                    # Actualizar nodo actual para la siguiente iteración
                    nodo_actual = linea_row['NODO2_ID']
                    encontrado_en_lineas = True

                    # Eliminar la línea procesada de la copia modificable
                    df_lineas_mod = df_lineas_mod[
                        (df_lineas_mod['G3E_FID'] != linea_row['G3E_FID'])
                    ].reset_index(drop=True)
                    break # Procesamos solo una línea a la vez para seguir el camino

            if encontrado_en_lineas:
                continue # Continuar el barrido con el nuevo nodo de línea

            # 2. Si no se encontraron más líneas, buscar en los elementos de corte
            elementos_corte_conectados = df_elementos_corte_mod[df_elementos_corte_mod['NODO1_ID'] == nodo_actual]

            if not elementos_corte_conectados.empty:
                for _, elemento_corte_row in elementos_corte_conectados.iterrows():
                    nuevo_elemento_corte = {
                        'G3E_FID': elemento_corte_row['G3E_FID'],
                        'NODO1_ID': elemento_corte_row['NODO1_ID'],
                        'NODO2_ID': elemento_corte_row['NODO2_ID'],
                        'CIRCUITO': elemento_corte_row['CIRCUITO'],
                        'CODIGO_OPERATIVO': elemento_corte_row['CODIGO_OPERATIVO'],
                        'EQUIPO_PADRE': codigo_operativo_padre,
                        'ELEMENTOS_AGUAS_ARRIBA_CORTE': ','.join(elementos_aguas_arriba_corte)
                    }
                    df_elementos_corte_resultado = pd.concat([df_elementos_corte_resultado, pd.DataFrame([nuevo_elemento_corte])], ignore_index=True)

                    # Actualizar nodo actual y el padre para la siguiente iteración
                    nodo_actual = elemento_corte_row['NODO2_ID']
                    codigo_operativo_padre = elemento_corte_row['CODIGO_OPERATIVO']
                    elementos_aguas_arriba_corte.append(codigo_operativo_padre)
                    encontrado_en_cortes = True

                    # Eliminar el elemento de corte procesado de la copia modificable
                    df_elementos_corte_mod = df_elementos_corte_mod[
                        (df_elementos_corte_mod['G3E_FID'] != elemento_corte_row['G3E_FID'])
                    ].reset_index(drop=True)
                    break # Procesamos solo un elemento de corte a la vez para seguir el camino

            if encontrado_en_cortes:
                continue # Continuar el barrido con el nuevo nodo de corte

            # Si no se encontró nada en líneas ni en elementos de corte, el barrido de este ramal ha terminado
            break

    return df_elementos_corte_resultado, df_lineas_resultado

# --- Carga de datos ---
try:
    df_circuitos = pd.read_excel('Data/Circuitos.xlsx', sheet_name="Prueba")
    df_elementos_corte = pd.read_excel('Data/elementos_corte.xlsx')
    df_lineas = pd.read_excel('Data/lineas.xlsx')
except FileNotFoundError as e:
    print(f"Error: No se pudo encontrar el archivo {e.filename}. Asegúrate de que los archivos Excel estén en la misma carpeta que el script.")
    exit()

# Ejecutar la función de barrido
df_resultados_corte, df_resultados_lineas = realizar_barrido_circuito(df_circuitos, df_elementos_corte, df_lineas)

# --- Mostrar resultados (opcional) ---
print("\n--- Resultados de Elementos de Corte ---")
print(df_resultados_corte)

print("\n--- Resultados de Líneas ---")
print(df_resultados_lineas)

# --- Guardar resultados en archivos Excel (opcional) ---
try:
    df_resultados_corte.to_excel('resultados_elementos_corte.xlsx', index=False)
    df_resultados_lineas.to_excel('resultados_lineas.xlsx', index=False)
    print("\nResultados guardados en 'resultados_elementos_corte.xlsx' y 'resultados_lineas.xlsx'")
except Exception as e:
    print(f"Error al guardar los resultados: {e}")