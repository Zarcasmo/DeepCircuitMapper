# -*- coding: utf-8 -*-
"""
Created on Thu May 22 13:57:58 2025

@author: alopagui
"""

import pandas as pd

# ---- Importar dependencias ----
from visualizacion_grafos import generar_grafo_circuito
# ------------------------------------

def cargar_datos(file_circuitos, file_elementos_corte, file_lineas):
    """
    Carga los datos desde los archivos Excel especificados.
    Convierte las columnas de NODO_ID, G3E_FID, CODIGO_OPERATIVO y Circuito a string 
    y elimina espacios en blanco al inicio/final para consistencia.
    """
    try:
        df_circuitos = pd.read_csv(file_circuitos, delimiter=";")
        df_elementos_corte = pd.read_csv(file_elementos_corte, delimiter=";")
        df_lineas = pd.read_csv(file_lineas, delimiter=";")
    except FileNotFoundError as e:
        print(f"❌ Error: Archivo no encontrado - {e.filename}")
        return None, None, None
    except Exception as e:
        print(f"❌ Error al leer los archivos Excel: {e}")
        return None, None, None

    columnas_esperadas_circuitos = ["Circuito"]
    columnas_esperadas_elementos_corte = ["G3E_FID", "NODO1_ID", "NODO2_ID", "CODIGO_OPERATIVO"]
    columnas_esperadas_lineas = ["G3E_FID", "NODO1_ID", "NODO2_ID"]

    if df_circuitos is not None:
        if not all(col in df_circuitos.columns for col in columnas_esperadas_circuitos):
            print(f"❌ Error: Faltan columnas en {file_circuitos}. Se esperan: {columnas_esperadas_circuitos}")
            return None, None, None
        df_circuitos['Circuito'] = df_circuitos['Circuito'].astype(str).str.strip()
    else: return None, None, None

    if df_elementos_corte is not None:
        if not all(col in df_elementos_corte.columns for col in columnas_esperadas_elementos_corte):
            print(f"❌ Error: Faltan columnas en {file_elementos_corte}. Se esperan: {columnas_esperadas_elementos_corte}")
            return None, None, None
        df_elementos_corte['G3E_FID'] = df_elementos_corte['G3E_FID'].astype(str).str.strip()
        df_elementos_corte['NODO1_ID'] = df_elementos_corte['NODO1_ID'].astype(str).str.strip()
        df_elementos_corte['NODO2_ID'] = df_elementos_corte['NODO2_ID'].astype(str).str.strip()
        df_elementos_corte['CODIGO_OPERATIVO'] = df_elementos_corte['CODIGO_OPERATIVO'].astype(str).str.strip()
    else: return None, None, None
        
    if df_lineas is not None:
        if not all(col in df_lineas.columns for col in columnas_esperadas_lineas):
            print(f"❌ Error: Faltan columnas en {file_lineas}. Se esperan: {columnas_esperadas_lineas}")
            return None, None, None
        df_lineas['G3E_FID'] = df_lineas['G3E_FID'].astype(str).str.strip()
        df_lineas['NODO1_ID'] = df_lineas['NODO1_ID'].astype(str).str.strip()
        df_lineas['NODO2_ID'] = df_lineas['NODO2_ID'].astype(str).str.strip()
    else: return None, None, None

    return df_circuitos, df_elementos_corte, df_lineas

def barrido_conectividad_por_circuito(
    circuito_co_inicial,
    df_elementos_corte_global,
    df_lineas_global,
    resultados_elementos_corte_global_lista,
    resultados_lineas_global_lista
    ):
    elementos_arranque = df_elementos_corte_global[df_elementos_corte_global['CODIGO_OPERATIVO'] == circuito_co_inicial]
    if elementos_arranque.empty:
        print(f"ℹ️ Advertencia: No se encontró el elemento de arranque con CODIGO_OPERATIVO '{circuito_co_inicial}' en el archivo de elementos de corte.")
        return

    elemento_arranque = elementos_arranque.iloc[0].copy()
    fid_arranque = str(elemento_arranque['G3E_FID'])

    visitados_ec_fids_este_circuito = set()
    visitados_lineas_fids_este_circuito = set()

    elemento_arranque_dict = elemento_arranque.to_dict()
    elemento_arranque_dict['Equipo_Padre'] = None 
    elemento_arranque_dict['Elementos_Aguas_Arriba'] = circuito_co_inicial
    elemento_arranque_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
    resultados_elementos_corte_global_lista.append(elemento_arranque_dict)
    visitados_ec_fids_este_circuito.add(fid_arranque)

    pila_exploracion = []
    camino_co_aguas_arriba_para_hijos_de_arranque = [circuito_co_inicial]

    if pd.notna(elemento_arranque['NODO1_ID']) and elemento_arranque['NODO1_ID'] != 'nan':
        pila_exploracion.append((str(elemento_arranque['G3E_FID']), elemento_arranque['TIPO'], str(elemento_arranque['NODO1_ID']), circuito_co_inicial, list(camino_co_aguas_arriba_para_hijos_de_arranque)))
    if pd.notna(elemento_arranque['NODO2_ID']) and elemento_arranque['NODO2_ID'] != 'nan':
        pila_exploracion.append((str(elemento_arranque['G3E_FID']), elemento_arranque['TIPO'], str(elemento_arranque['NODO2_ID']), circuito_co_inicial, list(camino_co_aguas_arriba_para_hijos_de_arranque)))

    while pila_exploracion:
        fid_actual, tipo_actual, nodo_actual, co_ec_padre_directo, camino_co_hasta_padre_directo = pila_exploracion.pop()

        lineas_conectadas = df_lineas_global[
            (df_lineas_global['NODO1_ID'] == nodo_actual) | (df_lineas_global['NODO2_ID'] == nodo_actual)
        ]
        for _, linea_conectada_row in lineas_conectadas.iterrows():
            linea_fid = str(linea_conectada_row['G3E_FID'])
            if linea_fid not in visitados_lineas_fids_este_circuito:
                visitados_lineas_fids_este_circuito.add(linea_fid)
                linea_dict = linea_conectada_row.to_dict()
                linea_dict['Equipo_Padre'] = co_ec_padre_directo
                linea_dict['Elementos_Aguas_Arriba'] = ",".join(camino_co_hasta_padre_directo)
                linea_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
                resultados_lineas_global_lista.append(linea_dict)
                otro_nodo_linea = None
                if str(linea_conectada_row['NODO1_ID']) == nodo_actual and pd.notna(linea_conectada_row['NODO2_ID']) and str(linea_conectada_row['NODO2_ID']) != 'nan':
                    otro_nodo_linea = str(linea_conectada_row['NODO2_ID'])
                elif str(linea_conectada_row['NODO2_ID']) == nodo_actual and pd.notna(linea_conectada_row['NODO1_ID']) and str(linea_conectada_row['NODO1_ID']) != 'nan':
                    otro_nodo_linea = str(linea_conectada_row['NODO1_ID'])
                if otro_nodo_linea:
                    pila_exploracion.append((linea_fid, 'LINEA', otro_nodo_linea, co_ec_padre_directo, list(camino_co_hasta_padre_directo)))

        ecs_conectados = df_elementos_corte_global[
            ((df_elementos_corte_global['NODO1_ID'] == nodo_actual) | (df_elementos_corte_global['NODO2_ID'] == nodo_actual))
        ]
        for _, ec_conectado_row in ecs_conectados.iterrows():
            ec_fid = str(ec_conectado_row['G3E_FID'])
            if ec_fid not in visitados_ec_fids_este_circuito:
                visitados_ec_fids_este_circuito.add(ec_fid)
                ec_dict = ec_conectado_row.to_dict()
                ec_dict['Equipo_Padre'] = co_ec_padre_directo
                ec_dict['Elementos_Aguas_Arriba'] = ",".join(camino_co_hasta_padre_directo)
                ec_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
                resultados_elementos_corte_global_lista.append(ec_dict)
                if ec_conectado_row['EST_ESTABLE']=='CLOSED':
                    nuevo_co_ec_padre_para_hijos = str(ec_conectado_row['CODIGO_OPERATIVO'])
                    nuevo_camino_co_para_hijos = list(camino_co_hasta_padre_directo) + [nuevo_co_ec_padre_para_hijos]
                    otro_nodo_ec = None
                    if str(ec_conectado_row['NODO1_ID']) == nodo_actual and pd.notna(ec_conectado_row['NODO1_ID']) and str(ec_conectado_row['NODO1_ID']) != 'nan':
                        otro_nodo_ec = str(ec_conectado_row['NODO2_ID'])
                    elif str(ec_conectado_row['NODO2_ID']) == nodo_actual and pd.notna(ec_conectado_row['NODO2_ID']) and str(ec_conectado_row['NODO2_ID']) != 'nan':
                        otro_nodo_ec = str(ec_conectado_row['NODO1_ID'])
                    pila_exploracion.append((ec_fid, ec_conectado_row['TIPO'], otro_nodo_ec, nuevo_co_ec_padre_para_hijos, nuevo_camino_co_para_hijos))
                    

def generar_dfs_resultados_finales(df_circuitos, df_elementos_corte_global, df_lineas_global):
    if df_circuitos is None or df_elementos_corte_global is None or df_lineas_global is None:
        print("❌ Error en la carga de datos inicial. No se puede continuar.")
        return None, None
    resultados_elementos_corte_acumulados_lista = []
    resultados_lineas_acumulados_lista = []
    for _, row_circuito in df_circuitos.iterrows():
        circuito_co_inicial = str(row_circuito['Circuito'])
        print(f"🔄 Procesando circuito: {circuito_co_inicial}...")
        barrido_conectividad_por_circuito(
            circuito_co_inicial,
            df_elementos_corte_global.loc[df_elementos_corte_global['CIRCUITO']==circuito_co_inicial],
            df_lineas_global.loc[df_lineas_global['CIRCUITO']==circuito_co_inicial],
            resultados_elementos_corte_acumulados_lista,
            resultados_lineas_acumulados_lista
        )
    df_final_elementos_corte = pd.DataFrame(resultados_elementos_corte_acumulados_lista)
    df_final_lineas = pd.DataFrame(resultados_lineas_acumulados_lista)
    if not df_final_elementos_corte.empty:
        cols_subset_ec = ['G3E_FID', 'Circuito_Origen_Barrido', 'Equipo_Padre', 'Elementos_Aguas_Arriba']
        cols_subset_ec_existentes = [col for col in cols_subset_ec if col in df_final_elementos_corte.columns]
        df_final_elementos_corte = df_final_elementos_corte.drop_duplicates(subset=cols_subset_ec_existentes)
    if not df_final_lineas.empty:
        cols_subset_li = ['G3E_FID', 'Circuito_Origen_Barrido', 'Equipo_Padre', 'Elementos_Aguas_Arriba']
        cols_subset_li_existentes = [col for col in cols_subset_li if col in df_final_lineas.columns]
        df_final_lineas = df_final_lineas.drop_duplicates(subset=cols_subset_li_existentes)
    return df_final_elementos_corte, df_final_lineas


# --- Inicio de la ejecución ---
if __name__ == "__main__":
    archivo_circuitos = "Data/circuitos.csv"
    archivo_elementos_corte = "Data/elementos_corte.csv"
    archivo_lineas = "Data/Lineas.csv" 

    print("🔌 Iniciando proceso de barrido de conectividad eléctrica...")
    
    df_circuitos, df_ecs, df_lins = cargar_datos(archivo_circuitos, archivo_elementos_corte, archivo_lineas)

    if df_circuitos is not None and df_ecs is not None and df_lins is not None:
        print("✅ Datos cargados exitosamente.")
        
        df_resultados_ecs, df_resultados_lins = generar_dfs_resultados_finales(df_circuitos, df_ecs, df_lins)

        if df_resultados_ecs is not None and df_resultados_lins is not None:
            print("\n🎉 ¡Barrido completado!")
            
            print("\n--- Resultados: Elementos de Corte (primeras filas) ---")
            if not df_resultados_ecs.empty:
                print(df_resultados_ecs.head())
                # df_resultados_ecs.to_excel("resultados_elementos_corte_final.xlsx", index=False)
                # print("\nResultados de elementos de corte guardados en 'resultados_elementos_corte_final.xlsx'")
            else:
                print("No se encontraron resultados para elementos de corte.")

            print("\n--- Resultados: Líneas (primeras filas) ---")
            if not df_resultados_lins.empty:
                print(df_resultados_lins.head())
                # df_resultados_lins.to_excel("resultados_lineas_final.xlsx", index=False)
                # print("\nResultados de líneas guardados en 'resultados_lineas_final.xlsx'")
            else:
                print("No se encontraron resultados para líneas.")

            # ---- SECCIÓN PARA GENERAR GRAFOS ----
            if not df_resultados_ecs.empty:
                print("\n📊 Iniciando generación de grafos para elementos de corte por circuito...")
                output_folder_grafos = "grafos_circuitos_ecs" 

                if 'Circuito_Origen_Barrido' in df_resultados_ecs.columns:
                    circuitos_con_resultados = df_resultados_ecs['Circuito_Origen_Barrido'].unique()
                    
                    for circuito_actual_co in circuitos_con_resultados:
                        print(f"   Generando grafo para el circuito: {circuito_actual_co}...")
                        df_circuito_especifico_ecs = df_resultados_ecs[
                            df_resultados_ecs['Circuito_Origen_Barrido'] == circuito_actual_co
                        ].copy()

                        if not df_circuito_especifico_ecs.empty:
                            # LLAMADA A LA FUNCIÓN IMPORTADA
                            generar_grafo_circuito(
                                df_datos_circuito=df_circuito_especifico_ecs,
                                circuito_co_origen=circuito_actual_co,
                                output_folder=output_folder_grafos,
                                # --- Parámetros de personalización ---
                                font_size=8,
                                line_thickness=0.8,
                                node_width=1.9,       # Ajustado ligeramente
                                node_height=0.5,      # Ajustado ligeramente
                                rankdir='TB',
                                default_node_color='lightsteelblue', # Color para nodos por defecto o estado desconocido
                                interruptor_principal_color='gold', # Amarillo para el interruptor principal (tipo 'I' en el origen)
                                estado_closed_color='mediumseagreen', # Verde para "CLOSED"
                                estado_open_color='tomato',         # Rojo para "OPEN"
                                edge_color='dimgray',
                                font_name='Helvetica' # O 'Arial', etc.
                            )
                        else:
                            print(f"   ℹ️ No hay datos de elementos de corte para el circuito {circuito_actual_co} para generar grafo.")
                else:
                    print("   ⚠️ No se encontró la columna 'Circuito_Origen_Barrido' en los resultados de elementos de corte.")
            # ---- FIN DE LA SECCIÓN DE GRAFOS ----
        else:
            print("⚠️ No se pudieron generar los DataFrames de resultados del barrido.")
    else:
        print("❌ Error en la carga de datos. El proceso no puede continuar.")