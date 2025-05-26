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

# --- Helper para obtener el otro nodo ---
def obtener_otro_nodo(row_elemento, nodo_conocido_str, col_nodo1='NODO1_ID', col_nodo2='NODO2_ID'):
    nodo1_str = str(row_elemento.get(col_nodo1, 'NAN')).upper()
    nodo2_str = str(row_elemento.get(col_nodo2, 'NAN')).upper()
    nodo_conocido_str = str(nodo_conocido_str).upper()

    if nodo1_str == nodo_conocido_str:
        return nodo2_str if nodo2_str != 'NAN' else None
    elif nodo2_str == nodo_conocido_str:
        return nodo1_str if nodo1_str != 'NAN' else None
    return None

# --- Fase 1: Barrido Principal y Recolección de Puntos de Anillo ---
def barrido_conectividad_circuito_principal(
    circuito_co_inicial, # CO del interruptor del circuito a barrer
    df_elementos_corte_global,
    df_lineas_global,
    resultados_ec_fase1_lista, # Lista para almacenar ECs encontrados en esta fase
    resultados_lineas_fase1_lista, # Lista para almacenar Líneas encontradas
    puntos_potenciales_anillo_lista # Lista para almacenar info de ECs "OPEN"
    ):

    elemento_arranque_filas = df_elementos_corte_global[df_elementos_corte_global['CODIGO_OPERATIVO'] == circuito_co_inicial]
    if elemento_arranque_filas.empty:
        print(f"ℹ️ Advertencia (Fase 1): No se encontró el elemento de arranque '{circuito_co_inicial}'.")
        return set(), set() # Devuelve sets vacíos de visitados

    elemento_arranque = elemento_arranque_filas.iloc[0].copy()
    fid_arranque = str(elemento_arranque['G3E_FID'])
    tipo_arranque = str(elemento_arranque['TIPO'])

    visitados_ec_fids_este_circuito = set()
    visitados_lineas_fids_este_circuito = set()

    # Registrar elemento de arranque
    ec_dict_arranque = elemento_arranque.to_dict()
    ec_dict_arranque['Equipo_Padre'] = None
    ec_dict_arranque['Elementos_Aguas_Arriba'] = circuito_co_inicial
    ec_dict_arranque['Circuito_Origen_Barrido'] = circuito_co_inicial
    ec_dict_arranque['Es_Parte_De_Anillo'] = False # Nueva columna
    resultados_ec_fase1_lista.append(ec_dict_arranque)
    visitados_ec_fids_este_circuito.add(fid_arranque)

    pila_exploracion = []
    camino_inicial = [circuito_co_inicial]
    nodo_arranque_principal = str(elemento_arranque.get('NODO2_ID', 'NAN')).upper() # Inicio por NODO2 como solicitado

    if nodo_arranque_principal != 'NAN':
        # Stack: (fid_elemento_actual, tipo_elemento_actual, nodo_a_explorar_desde, co_padre_directo, camino_co_hasta_padre)
        pila_exploracion.append((fid_arranque, tipo_arranque, nodo_arranque_principal, circuito_co_inicial, list(camino_inicial)))
    else:
        print(f"⚠️ Advertencia (Fase 1): Elemento de arranque '{circuito_co_inicial}' no tiene NODO2_ID válido.")
        return visitados_ec_fids_este_circuito, visitados_lineas_fids_este_circuito


    while pila_exploracion:
        fid_elem_actual, tipo_elem_actual, nodo_actual, co_ec_padre_directo, camino_co_hasta_padre = pila_exploracion.pop()

        # A. Buscar Líneas Conectadas
        lineas_conectadas = df_lineas_global[
            (df_lineas_global['NODO1_ID'] == nodo_actual) | (df_lineas_global['NODO2_ID'] == nodo_actual)
        ]
        for _, linea_row in lineas_conectadas.iterrows():
            linea_fid = str(linea_row['G3E_FID'])
            if linea_fid not in visitados_lineas_fids_este_circuito:
                visitados_lineas_fids_este_circuito.add(linea_fid)
                linea_dict = linea_row.to_dict()
                linea_dict['Equipo_Padre'] = co_ec_padre_directo
                linea_dict['Elementos_Aguas_Arriba'] = ",".join(camino_co_hasta_padre)
                linea_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
                linea_dict['Es_Parte_De_Anillo'] = False
                resultados_lineas_fase1_lista.append(linea_dict)
                
                otro_nodo_linea = obtener_otro_nodo(linea_row, nodo_actual)
                if otro_nodo_linea:
                    pila_exploracion.append((linea_fid, 'LINEA', otro_nodo_linea, co_ec_padre_directo, list(camino_co_hasta_padre)))

        # B. Buscar Elementos de Corte Conectados
        ecs_conectados = df_elementos_corte_global[
            ((df_elementos_corte_global['NODO1_ID'] == nodo_actual) | (df_elementos_corte_global['NODO2_ID'] == nodo_actual)) &
            (df_elementos_corte_global['G3E_FID'] != fid_elem_actual) # Evitar auto-conexión simple si el EC tiene ambos nodos al nodo_actual
        ]
        for _, ec_row in ecs_conectados.iterrows():
            ec_fid = str(ec_row['G3E_FID'])
            if ec_fid not in visitados_ec_fids_este_circuito:
                visitados_ec_fids_este_circuito.add(ec_fid)
                ec_dict = ec_row.to_dict()
                ec_dict['Equipo_Padre'] = co_ec_padre_directo
                ec_dict['Elementos_Aguas_Arriba'] = ",".join(camino_co_hasta_padre)
                ec_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
                ec_dict['Es_Parte_De_Anillo'] = False
                resultados_ec_fase1_lista.append(ec_dict)

                estado_ec = str(ec_row.get('EST_ESTABLE', 'UNKNOWN')).upper()
                co_ec_actual = str(ec_row['CODIGO_OPERATIVO'])
                tipo_ec_actual = str(ec_row['TIPO'])

                if estado_ec == 'CLOSED':
                    nuevo_camino_co = list(camino_co_hasta_padre) + [co_ec_actual]
                    # Explorar desde ambos nodos del EC cerrado si son distintos del nodo_actual de llegada
                    # O, como en tu lógica, desde el "otro" nodo.
                    otro_nodo_de_ec_cerrado = obtener_otro_nodo(ec_row, nodo_actual)
                    if otro_nodo_de_ec_cerrado:
                         pila_exploracion.append((ec_fid, tipo_ec_actual, otro_nodo_de_ec_cerrado, co_ec_actual, nuevo_camino_co))
                    # Si se quisiera explorar ambos nodos del EC cerrado:
                    # for nodo_ec_key in ['NODO1_ID', 'NODO2_ID']:
                    #    nodo_a_explorar_ec = str(ec_row.get(nodo_ec_key, 'NAN')).upper()
                    #    if nodo_a_explorar_ec != 'NAN':
                    #        pila_exploracion.append((ec_fid, tipo_ec_actual, nodo_a_explorar_ec, co_ec_actual, nuevo_camino_co))
                
                elif estado_ec == 'OPEN':
                    nodo_para_explorar_anillo = obtener_otro_nodo(ec_row, nodo_actual)
                    if nodo_para_explorar_anillo: # Solo si tiene un "otro" nodo válido
                        puntos_potenciales_anillo_lista.append({
                            'ec_open_fid': ec_fid,
                            'ec_open_co': co_ec_actual,
                            'nodo_explorar_anillo': nodo_para_explorar_anillo,
                            'circuito_origen_co': circuito_co_inicial,
                            'camino_co_hasta_open': list(camino_co_hasta_padre) + [co_ec_actual],
                            'fids_rama_principal_en_este_punto': set(visitados_ec_fids_este_circuito | visitados_lineas_fids_este_circuito) # Copia del estado actual de visitados
                        })
    return visitados_ec_fids_este_circuito, visitados_lineas_fids_este_circuito


# --- Fase 2: Búsqueda de Caminos de Anillo ---
def buscar_camino_anillo_dfs(
    nodo_de_partida_anillo,
    co_elemento_open, # CO del EC "OPEN" que origina esta búsqueda
    camino_co_hasta_open, # Lista de COs hasta el EC "OPEN"
    circuito_origen_principal, # CO del circuito donde se encontró el EC "OPEN"
    fids_visitados_rama_principal, # Set de G3E_FIDs de la Fase 1 de este circuito
    df_elementos_corte_global,
    df_lineas_global,
    df_circuitos_todos # DataFrame de todos los circuitos válidos
    ):
    
    print(f"    🔎 Buscando anillo desde EC OPEN '{co_elemento_open}' (nodo: {nodo_de_partida_anillo}) del circuito '{circuito_origen_principal}'...")
    
    pila_anillo = []
    # Stack: (fid_elem_actual, tipo_elem_actual, nodo_a_explorar, co_padre_directo_anillo, camino_co_anillo, fids_camino_anillo_actual)
    # El padre inicial para el camino del anillo es el propio EC OPEN.
    # El fid_elem_actual y tipo_elem_actual iniciales son los del EC OPEN.
    ec_open_row = df_elementos_corte_global[df_elementos_corte_global['CODIGO_OPERATIVO'] == co_elemento_open].iloc[0] # Asumimos que existe y es único
    
    pila_anillo.append((
        str(ec_open_row['G3E_FID']), 
        str(ec_open_row['TIPO']), 
        nodo_de_partida_anillo, 
        co_elemento_open, 
        list(camino_co_hasta_open), # El camino ya incluye al EC OPEN
        [] # Lista de FIDs de elementos nuevos en este camino de anillo
    ))

    visitados_fids_este_anillo_sweep = set() # Para evitar ciclos DENTRO de la búsqueda de este anillo específico
    elementos_nuevos_en_camino_anillo = [] # [{elemento_dict, tipo: 'EC'/'LINEA'}, ...]

    max_profundidad_anillo = 200 # Límite para evitar exploraciones muy largas en redes complejas
    iteraciones = 0

    while pila_anillo and iteraciones < max_profundidad_anillo:
        iteraciones += 1
        fid_elem_actual_anillo, tipo_elem_actual_anillo, nodo_actual_anillo, co_padre_anillo, camino_co_actual_anillo, fids_path_anillo = pila_anillo.pop()

        if fid_elem_actual_anillo in visitados_fids_este_anillo_sweep and tipo_elem_actual_anillo != 'EC_OPEN_START': # EC_OPEN_START es un pseudo tipo para el primer elemento
            continue # Ya exploramos este elemento en este intento de anillo
        if tipo_elem_actual_anillo != 'EC_OPEN_START':
             visitados_fids_este_anillo_sweep.add(fid_elem_actual_anillo)


        # A. Buscar Líneas Conectadas en el camino del anillo
        lineas_conectadas_anillo = df_lineas_global[
            ((df_lineas_global['NODO1_ID'] == nodo_actual_anillo) | (df_lineas_global['NODO2_ID'] == nodo_actual_anillo)) &
            (~df_lineas_global['G3E_FID'].isin(fids_visitados_rama_principal)) # No volver por la rama principal energizada
        ]
        for _, linea_row_anillo in lineas_conectadas_anillo.iterrows():
            linea_fid_anillo = str(linea_row_anillo['G3E_FID'])
            if linea_fid_anillo not in visitados_fids_este_anillo_sweep and linea_fid_anillo not in fids_path_anillo:
                linea_dict_anillo = linea_row_anillo.to_dict()
                linea_dict_anillo['Equipo_Padre'] = co_padre_anillo # El padre en el contexto del camino del anillo
                linea_dict_anillo['Elementos_Aguas_Arriba'] = ",".join(camino_co_actual_anillo)
                linea_dict_anillo['Circuito_Origen_Barrido'] = circuito_origen_principal # Sigue siendo del circuito original
                linea_dict_anillo['Es_Parte_De_Anillo'] = True
                
                current_path_elements = elementos_nuevos_en_camino_anillo + [{'data': linea_dict_anillo, 'tipo_elem': 'LINEA'}]
                
                otro_nodo_linea_anillo = obtener_otro_nodo(linea_row_anillo, nodo_actual_anillo)
                if otro_nodo_linea_anillo:
                    pila_anillo.append((
                        linea_fid_anillo, 'LINEA', otro_nodo_linea_anillo, 
                        co_padre_anillo, # El padre no cambia al pasar por una línea
                        list(camino_co_actual_anillo),
                        fids_path_anillo + [linea_fid_anillo]
                    ))
                    # Temporalmente añadir a elementos_nuevos_en_camino_anillo para no perderlo si este no es el final
                    # Se confirmará si es un camino válido al final de la función si se encuentra un cierre.
                    # Esto se complica, es mejor construir la lista de elementos solo cuando se confirma el anillo.
                    # Por ahora, la función solo retornará el punto de cierre.
                    # La adición de elementos intermedios al DF global se manejará después.


        # B. Buscar Elementos de Corte Conectados en el camino del anillo
        ecs_conectados_anillo = df_elementos_corte_global[
             (((df_elementos_corte_global['NODO1_ID'] == nodo_actual_anillo) | (df_elementos_corte_global['NODO2_ID'] == nodo_actual_anillo))) &
             (df_elementos_corte_global['G3E_FID'] != fid_elem_actual_anillo) & # No el mismo del que partimos en este paso
             (~df_elementos_corte_global['G3E_FID'].isin(fids_path_anillo)) # No parte del camino actual de este anillo
        ]
        for _, ec_row_anillo in ecs_conectados_anillo.iterrows():
            ec_fid_anillo = str(ec_row_anillo['G3E_FID'])
            ec_co_anillo = str(ec_row_anillo['CODIGO_OPERATIVO'])
            ec_circuito_nativo = str(ec_row_anillo.get('CIRCUITO', 'UNKNOWN_CIRCUIT')).upper() # Circuito al que pertenece este EC

            # Condición 1: Cierra anillo con la rama principal del mismo circuito
            if ec_fid_anillo in fids_visitados_rama_principal:
                print(f"    ✅ Anillo INTERNO detectado: EC OPEN '{co_elemento_open}' cierra con '{ec_co_anillo}' (del mismo circuito '{circuito_origen_principal}')")
                # Aquí se deberían registrar los elementos intermedios de este camino de anillo
                return {'tipo': 'INTERNO', 'circuito_cierre_co': circuito_origen_principal, 'elemento_cierre_co': ec_co_anillo, 'elementos_camino': fids_path_anillo + [ec_fid_anillo] }

            # Condición 2: Cierra anillo con OTRO circuito
            if ec_circuito_nativo != circuito_origen_principal and \
               ec_circuito_nativo in df_circuitos_todos['Circuito'].unique():
                print(f"    ✅ Anillo EXTERNO detectado: EC OPEN '{co_elemento_open}' (de Cto '{circuito_origen_principal}') cierra con '{ec_co_anillo}' (del Cto '{ec_circuito_nativo}')")
                return {'tipo': 'EXTERNO', 'circuito_cierre_co': ec_circuito_nativo, 'elemento_cierre_co': ec_co_anillo, 'elementos_camino': fids_path_anillo + [ec_fid_anillo]}

            # Si no es un cierre y no ha sido visitado en ESTE barrido de anillo, y está CERRADO, continuar explorando
            if ec_fid_anillo not in visitados_fids_este_anillo_sweep:
                estado_ec_anillo = str(ec_row_anillo.get('EST_ESTABLE', 'UNKNOWN')).upper()
                if estado_ec_anillo == 'CLOSED':
                    # visitados_fids_este_anillo_sweep.add(ec_fid_anillo) # Marcar como visitado para este camino
                    nuevo_camino_co_anillo = list(camino_co_actual_anillo) + [ec_co_anillo]
                    otro_nodo_ec_anillo = obtener_otro_nodo(ec_row_anillo, nodo_actual_anillo)
                    if otro_nodo_ec_anillo:
                        pila_anillo.append((
                            ec_fid_anillo, str(ec_row_anillo['TIPO']), otro_nodo_ec_anillo,
                            ec_co_anillo, # Nuevo padre en el camino del anillo
                            nuevo_camino_co_anillo,
                            fids_path_anillo + [ec_fid_anillo]
                        ))
    
    print(f"    ℹ️ No se encontró cierre de anillo claro para EC OPEN '{co_elemento_open}' desde nodo {nodo_de_partida_anillo} (profundidad máx alcanzada o sin camino).")
    return None # No se encontró cierre o se alcanzó límite

# --- Orquestador Principal ---
def generar_dfs_resultados_con_anillos(df_circuitos, df_elementos_corte_global, df_lineas_global):
    if df_circuitos is None or df_elementos_corte_global is None or df_lineas_global is None:
        return None, None
        
    resultados_ecs_final_lista = []
    resultados_lineas_final_lista = []
    
    mapa_fids_ec_info_anillo = {} # Para actualizar ECs OPEN con info del anillo

    for _, row_circuito in df_circuitos.iterrows():
        circuito_co_actual = str(row_circuito['Circuito']).upper()
        print(f"🔄 Procesando Fase 1 para circuito: {circuito_co_actual}...")
        
        ecs_fase1_circuito_actual = []
        lineas_fase1_circuito_actual = []
        puntos_potenciales_anillo_circuito_actual = []

        # Ejecutar Fase 1
        fids_ec_visitados_fase1, fids_lineas_visitadas_fase1 = barrido_conectividad_circuito_principal(
            circuito_co_actual,
            df_elementos_corte_global,
            df_lineas_global,
            ecs_fase1_circuito_actual,
            lineas_fase1_circuito_actual,
            puntos_potenciales_anillo_circuito_actual
        )
        
        resultados_ecs_final_lista.extend(ecs_fase1_circuito_actual)
        resultados_lineas_final_lista.extend(lineas_fase1_circuito_actual)

        print(f"🔩 Procesando Fase 2 (Anillos) para circuito: {circuito_co_actual}...")
        fids_globales_rama_principal_actual = fids_ec_visitados_fase1 | fids_lineas_visitadas_fase1

        for punto_anillo in puntos_potenciales_anillo_circuito_actual:
            if punto_anillo['circuito_origen_co'] == circuito_co_actual: # Asegurar que es del circuito actual
                info_cierre = buscar_camino_anillo_dfs(
                    punto_anillo['nodo_explorar_anillo'],
                    punto_anillo['ec_open_co'],
                    punto_anillo['camino_co_hasta_open'],
                    circuito_co_actual,
                    punto_anillo['fids_rama_principal_en_este_punto'], # Usar el set de visitados en el momento que se encontró el OPEN
                    df_elementos_corte_global,
                    df_lineas_global,
                    df_circuitos # Pasar todos los circuitos para validación
                )
                if info_cierre:
                    mapa_fids_ec_info_anillo[punto_anillo['ec_open_fid']] = {
                        'Anillo_Tipo': info_cierre['tipo'],
                        'Anillo_Con_Circuito_CO': info_cierre['circuito_cierre_co'],
                        'Anillo_Con_Elemento_CO': info_cierre['elemento_cierre_co'],
                        # 'Anillo_Camino_Elementos_FIDs': info_cierre['elementos_camino'] # Podríamos guardar esto si es necesario
                    }
                    # Aquí podríamos añadir los elementos del 'elementos_camino' a las listas globales de resultados
                    # marcándolos como 'Es_Parte_De_Anillo' = True, pero esto requiere más lógica
                    # para construir su 'Equipo_Padre' y 'Elementos_Aguas_Arriba' en el contexto del camino del anillo.
                    # Por simplicidad, por ahora solo se marca el EC OPEN.

    df_final_elementos_corte = pd.DataFrame(resultados_ecs_final_lista)
    df_final_lineas = pd.DataFrame(resultados_lineas_final_lista)

    # Añadir información de anillos a los ECs OPEN
    if not df_final_elementos_corte.empty and mapa_fids_ec_info_anillo:
        df_final_elementos_corte['Anillo_Tipo'] = df_final_elementos_corte['G3E_FID'].map(
            lambda fid: mapa_fids_ec_info_anillo.get(fid, {}).get('Anillo_Tipo')
        )
        df_final_elementos_corte['Anillo_Con_Circuito_CO'] = df_final_elementos_corte['G3E_FID'].map(
            lambda fid: mapa_fids_ec_info_anillo.get(fid, {}).get('Anillo_Con_Circuito_CO')
        )
        df_final_elementos_corte['Anillo_Con_Elemento_CO'] = df_final_elementos_corte['G3E_FID'].map(
            lambda fid: mapa_fids_ec_info_anillo.get(fid, {}).get('Anillo_Con_Elemento_CO')
        )

    # Limpieza de duplicados (puede ser necesario si elementos son añadidos de múltiples formas)
    # ... (lógica de drop_duplicates si es necesaria) ...
            
    return df_final_elementos_corte, df_final_lineas


# --- Bloque Principal de Ejecución ---
if __name__ == "__main__":
    archivo_circuitos = "Data/circuitos.csv"
    archivo_elementos_corte = "Data/elementos_corte.csv"
    archivo_lineas = "Data/Lineas.csv" 

    print("🔌 Iniciando proceso de barrido de conectividad eléctrica...")
    df_circuitos_main, df_ecs_main, df_lins_main = cargar_datos(
        archivo_circuitos, archivo_elementos_corte, archivo_lineas
    )

    if df_circuitos_main is not None and df_ecs_main is not None and df_lins_main is not None:
        print("✅ Datos cargados exitosamente.")
        
        df_resultados_ecs, df_resultados_lins = generar_dfs_resultados_con_anillos(
            df_circuitos_main, df_ecs_main, df_lins_main
        )

        if df_resultados_ecs is not None: # df_resultados_lins puede estar vacío
            print("\n🎉 ¡Barrido con detección de anillos completado!")
            
            print("\n--- Resultados: Elementos de Corte (primeras filas) ---")
            if not df_resultados_ecs.empty:
                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', 1000)
                print(df_resultados_ecs.head())
                # df_resultados_ecs.to_excel("resultados_elementos_corte_con_anillos.xlsx", index=False)
                # print("\nResultados de ECs guardados en 'resultados_elementos_corte_con_anillos.xlsx'")
            else:
                print("No se encontraron resultados para elementos de corte.")

            print("\n--- Resultados: Líneas (primeras filas) ---")
            if df_resultados_lins is not None and not df_resultados_lins.empty :
                print(df_resultados_lins.head())
                # df_resultados_lins.to_excel("resultados_lineas_con_anillos.xlsx", index=False)
                # print("\nResultados de líneas guardados en 'resultados_lineas_con_anillos.xlsx'")
            else:
                print("No se encontraron resultados para líneas, o el DataFrame es None.")

            # ---- SECCIÓN PARA GENERAR GRAFOS (la llamada a generar_grafo_circuito no cambia) ----
            if not df_resultados_ecs.empty:
                print("\n📊 Iniciando generación de grafos...")
                output_folder_grafos = "grafos_circuitos_con_anillos" 
                if 'Circuito_Origen_Barrido' in df_resultados_ecs.columns:
                    circuitos_procesados_para_grafo = df_resultados_ecs['Circuito_Origen_Barrido'].unique()
                    for c_co in circuitos_procesados_para_grafo:
                        df_grafo_actual = df_resultados_ecs[df_resultados_ecs['Circuito_Origen_Barrido'] == c_co].copy()
                        # Aquí también se deberían pasar las líneas del circuito actual si el grafo las va a usar.
                        # Por ahora, la función de grafo solo usa df_resultados_ecs.
                        # Y también los elementos que forman el camino del anillo.
                        # Esta parte requiere que generar_grafo_circuito pueda manejar la info de anillos.
                        generar_grafo_circuito( # De visualizacion_grafos.py
                            df_datos_circuito_ecs=df_grafo_actual, # Pasar ECs
                            # df_datos_lineas=df_resultados_lins[df_resultados_lins['Circuito_Origen_Barrido'] == c_co].copy(), # Pasar Líneas (si es necesario)
                            circuito_co_origen=c_co,
                            output_folder=output_folder_grafos
                            # ... otros parámetros de estilo ...
                        )
        else:
            print("⚠️ No se pudieron generar los DataFrames de resultados del barrido.")
    else:
        print("❌ Error en la carga de datos. El proceso no puede continuar.")