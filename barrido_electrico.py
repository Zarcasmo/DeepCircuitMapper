import pandas as pd

# ---- Importar dependencias ----
from visualizacion_grafos import generar_grafo_circuito
from Data_process import cargar_datos
# ------------------------------------
data_load = "oracle"
#data_load="CSV"

def barrido_conectividad_por_circuito(
    circuito_co_inicial,
    df_elementos_corte_circuito, # Ya filtrado para el circuito de arranque
    df_lineas_circuito,          # Ya filtrado para el circuito de arranque
    resultados_elementos_corte_global_lista,
    resultados_lineas_global_lista
    ):
    elementos_arranque = df_elementos_corte_circuito[df_elementos_corte_circuito['CODIGO_OPERATIVO'] == circuito_co_inicial]
    if elementos_arranque.empty:
        print(f"ℹ️ Advertencia: No se encontró el elemento de arranque con CODIGO_OPERATIVO '{circuito_co_inicial}' en los elementos del circuito.")
        return

    elemento_arranque = elementos_arranque.iloc[0].copy()
    fid_arranque = str(elemento_arranque['G3E_FID'])

    visitados_ec_fids_este_circuito = set()
    visitados_lineas_fids_este_circuito = set()

    elemento_arranque_dict = elemento_arranque.to_dict()
    elemento_arranque_dict['Equipo_Padre'] = None 
    elemento_arranque_dict['Elementos_Aguas_Arriba'] = circuito_co_inicial
    elemento_arranque_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
    elemento_arranque_dict['Nodo_No_Explorado_Anillo'] = pd.NA # Inicializar para todos
    resultados_elementos_corte_global_lista.append(elemento_arranque_dict)
    visitados_ec_fids_este_circuito.add(fid_arranque)

    pila_exploracion = []
    camino_co_aguas_arriba_para_hijos_de_arranque = [circuito_co_inicial]

    # Añadir nodos del elemento de arranque a la pila (Los interruptores siempre parten del NODO 2)
    if pd.notna(elemento_arranque['NODO2_ID']) and elemento_arranque['NODO2_ID'] != 'nan':
        pila_exploracion.append((str(elemento_arranque['G3E_FID']), elemento_arranque['TIPO'], str(elemento_arranque['NODO2_ID']), circuito_co_inicial, list(camino_co_aguas_arriba_para_hijos_de_arranque)))

    while pila_exploracion:
        fid_actual, tipo_actual, nodo_actual, co_ec_padre_directo, camino_co_hasta_padre_directo = pila_exploracion.pop()

        # Explorar Líneas conectadas al nodo_actual
        # Usar los dataframes globales para encontrar todas las conexiones posibles
        lineas_conectadas = df_lineas_circuito[
            (df_lineas_circuito['NODO1_ID'] == nodo_actual) | (df_lineas_circuito['NODO2_ID'] == nodo_actual)
        ]
        for _, linea_conectada_row_original in lineas_conectadas.iterrows():
            linea_conectada_row = linea_conectada_row_original.copy()
            linea_fid = str(linea_conectada_row['G3E_FID'])
            if linea_fid not in visitados_lineas_fids_este_circuito:
                visitados_lineas_fids_este_circuito.add(linea_fid)
                linea_dict = linea_conectada_row.to_dict()
                linea_dict['Equipo_Padre'] = co_ec_padre_directo
                linea_dict['Elementos_Aguas_Arriba'] = ",".join(camino_co_hasta_padre_directo)
                linea_dict['Circuito_Origen_Barrido'] = circuito_co_inicial # El circuito que inició ESTE barrido
                resultados_lineas_global_lista.append(linea_dict)
                
                otro_nodo_linea = None
                if str(linea_conectada_row['NODO1_ID']) == nodo_actual and pd.notna(linea_conectada_row['NODO2_ID']) and str(linea_conectada_row['NODO2_ID']) != 'nan':
                    otro_nodo_linea = str(linea_conectada_row['NODO2_ID'])
                elif str(linea_conectada_row['NODO2_ID']) == nodo_actual and pd.notna(linea_conectada_row['NODO1_ID']) and str(linea_conectada_row['NODO1_ID']) != 'nan':
                    otro_nodo_linea = str(linea_conectada_row['NODO1_ID'])
                
                if otro_nodo_linea:
                    pila_exploracion.append((linea_fid, 'LINEA', otro_nodo_linea, co_ec_padre_directo, list(camino_co_hasta_padre_directo)))

        # Explorar Elementos de Corte conectados al nodo_actual
        ecs_conectados = df_elementos_corte_circuito[
            ((df_elementos_corte_circuito['NODO1_ID'] == nodo_actual) | (df_elementos_corte_circuito['NODO2_ID'] == nodo_actual)) &
            (df_elementos_corte_circuito['G3E_FID'] != fid_actual) # No reconectar al mismo EC desde el que se sale por un nodo
        ]
        for _, ec_conectado_row_original in ecs_conectados.iterrows():
            ec_conectado_row = ec_conectado_row_original.copy()
            ec_fid = str(ec_conectado_row['G3E_FID'])
            
            if ec_fid not in visitados_ec_fids_este_circuito:
                visitados_ec_fids_este_circuito.add(ec_fid)
                ec_dict = ec_conectado_row.to_dict()
                ec_dict['Equipo_Padre'] = co_ec_padre_directo
                ec_dict['Elementos_Aguas_Arriba'] = ",".join(camino_co_hasta_padre_directo)
                ec_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
                ec_dict['Nodo_No_Explorado_Anillo'] = pd.NA # Inicializar

                # Determinar el nodo no explorado si el EC está ABIERTO o conecta con otro circuito
                #En caso de que el EC conecte con otro circuito, se hace un artificio, y se asegura que su estado este OPEN para un correcto analisis del anillo que se forma
                if (ec_conectado_row['CIRCUITO'] != circuito_co_inicial):
                    ec_conectado_row['EST_ESTABLE'] = 'OPEN'
                    
                nodo_no_explorado_para_anillo = pd.NA
                if ec_conectado_row['EST_ESTABLE'] == 'OPEN':
                    if str(ec_conectado_row['NODO1_ID']) == nodo_actual and pd.notna(ec_conectado_row['NODO2_ID']) and str(ec_conectado_row['NODO2_ID']) != 'nan':
                        nodo_no_explorado_para_anillo = str(ec_conectado_row['NODO2_ID'])
                    elif str(ec_conectado_row['NODO2_ID']) == nodo_actual and pd.notna(ec_conectado_row['NODO1_ID']) and str(ec_conectado_row['NODO1_ID']) != 'nan':
                        nodo_no_explorado_para_anillo = str(ec_conectado_row['NODO1_ID'])
                    ec_dict['Nodo_No_Explorado_Anillo'] = nodo_no_explorado_para_anillo
                
                resultados_elementos_corte_global_lista.append(ec_dict)

                if ec_conectado_row['EST_ESTABLE'] == 'CLOSED': # Solo continuar barrido si está cerrado
                    nuevo_co_ec_padre_para_hijos = str(ec_conectado_row['CODIGO_OPERATIVO'])
                    nuevo_camino_co_para_hijos = list(camino_co_hasta_padre_directo) + [nuevo_co_ec_padre_para_hijos]
                    
                    otro_nodo_ec_para_explorar = pd.NA
                    # El nodo por el que se continúa es el que NO es 'nodo_actual'
                    if str(ec_conectado_row['NODO1_ID']) == nodo_actual and pd.notna(ec_conectado_row['NODO2_ID']) and str(ec_conectado_row['NODO2_ID']) != 'nan':
                        otro_nodo_ec_para_explorar = str(ec_conectado_row['NODO2_ID'])
                    elif str(ec_conectado_row['NODO2_ID']) == nodo_actual and pd.notna(ec_conectado_row['NODO1_ID']) and str(ec_conectado_row['NODO1_ID']) != 'nan':
                        otro_nodo_ec_para_explorar = str(ec_conectado_row['NODO1_ID'])
                    
                    if pd.notna(otro_nodo_ec_para_explorar):
                         pila_exploracion.append((ec_fid, ec_conectado_row['TIPO'], otro_nodo_ec_para_explorar, nuevo_co_ec_padre_para_hijos, nuevo_camino_co_para_hijos))

def barrido_anillos_especifico(
    co_ec_open_original,
    nodo_inicio_anillo,
    df_elementos_corte_global, 
    df_lineas_global,         
    df_resultados_ecs_completos 
):
    """
    Realiza un barrido desde el nodo_inicio_anillo de un EC 'OPEN' 
    para encontrar el primer EC conectado.
    """
    pila_exploracion_anillo = []
    visitados_fids_este_anillo = set() 
    visitados_fids_este_anillo.add(co_ec_open_original) # No "encontrar" el mismo EC OPEN como anillo

    # (fid_padre, tipo_padre, nodo_a_explorar)
    pila_exploracion_anillo.append( (None, "NODO_INICIAL_ANILLO", nodo_inicio_anillo) )

    max_iteraciones_anillo = 100 # Límite para evitar bucles infinitos en casos complejos
    iter_count = 0

    while pila_exploracion_anillo and iter_count < max_iteraciones_anillo:
        iter_count += 1
        fid_padre_actual, tipo_padre_actual, nodo_actual = pila_exploracion_anillo.pop()

        # 1. Buscar ECs Conectados directamente al nodo_actual
        ecs_conectados_directo_anillo = df_elementos_corte_global[
            ((df_elementos_corte_global['NODO1_ID'] == nodo_actual) | (df_elementos_corte_global['NODO2_ID'] == nodo_actual)) &
            (df_elementos_corte_global['CODIGO_OPERATIVO'] != co_ec_open_original) 
        ]
        for _, ec_row in ecs_conectados_directo_anillo.iterrows():
            co_ec_encontrado = str(ec_row['CODIGO_OPERATIVO'])
            # ¡EC encontrado!
            info_ec_encontrado_en_barrido1 = df_resultados_ecs_completos[
                df_resultados_ecs_completos['CODIGO_OPERATIVO'] == co_ec_encontrado
            ]
            if not info_ec_encontrado_en_barrido1.empty:
                data_ec_encontrado = info_ec_encontrado_en_barrido1.iloc[0]
                elementos_aguas_arriba_anillo = data_ec_encontrado.get('Elementos_Aguas_Arriba', pd.NA)
                circuito_origen_anillo = data_ec_encontrado.get('Circuito_Origen_Barrido', pd.NA)
                return co_ec_encontrado, elementos_aguas_arriba_anillo, circuito_origen_anillo
            else: # Encontrado en la red pero no en resultados del barrido (raro)
                return co_ec_encontrado, pd.NA, pd.NA # Retornar al menos el CO

        # 2. Si no hay EC directo, buscar Líneas Conectadas al nodo_actual para seguir explorando
        lineas_conectadas_anillo = df_lineas_global[
            ((df_lineas_global['NODO1_ID'] == nodo_actual) | (df_lineas_global['NODO2_ID'] == nodo_actual)) 
        ]
        for _, linea_row in lineas_conectadas_anillo.iterrows():
            linea_fid = str(linea_row['G3E_FID'])
            if linea_fid not in visitados_fids_este_anillo:
                visitados_fids_este_anillo.add(linea_fid)
                otro_nodo_linea = None
                if str(linea_row['NODO1_ID']) == nodo_actual and pd.notna(linea_row['NODO2_ID']) and str(linea_row['NODO2_ID']) != 'nan':
                    otro_nodo_linea = str(linea_row['NODO2_ID'])
                elif str(linea_row['NODO2_ID']) == nodo_actual and pd.notna(linea_row['NODO1_ID']) and str(linea_row['NODO1_ID']) != 'nan':
                    otro_nodo_linea = str(linea_row['NODO1_ID'])
                
                if otro_nodo_linea:
                    pila_exploracion_anillo.append((linea_fid, 'LINEA_ANILLO', otro_nodo_linea))
        
        # 3. Si no hay línea directa, buscar ECs CERRADOS para continuar la exploración (si el EC directo no fue el objetivo)
        # Esta parte es para atravesar ECs cerrados en el camino hacia el EC que forma el anillo
        ecs_conectados_para_atravesar = df_elementos_corte_global[
            ((df_elementos_corte_global['NODO1_ID'] == nodo_actual) | (df_elementos_corte_global['NODO2_ID'] == nodo_actual)) &
            (df_elementos_corte_global['CODIGO_OPERATIVO'] != co_ec_open_original) &
            (df_elementos_corte_global['EST_ESTABLE'] == 'CLOSED') # Solo atravesar cerrados
        ]
        for _, ec_row_atravesar in ecs_conectados_para_atravesar.iterrows():
            ec_fid_atravesar = str(ec_row_atravesar['G3E_FID'])
            if ec_fid_atravesar not in visitados_fids_este_anillo:
                visitados_fids_este_anillo.add(ec_fid_atravesar)
                otro_nodo_ec_atravesar = None
                if str(ec_row_atravesar['NODO1_ID']) == nodo_actual and pd.notna(ec_row_atravesar['NODO2_ID']) and str(ec_row_atravesar['NODO2_ID']) != 'nan':
                    otro_nodo_ec_atravesar = str(ec_row_atravesar['NODO2_ID'])
                elif str(ec_row_atravesar['NODO2_ID']) == nodo_actual and pd.notna(ec_row_atravesar['NODO1_ID']) and str(ec_row_atravesar['NODO1_ID']) != 'nan':
                    otro_nodo_ec_atravesar = str(ec_row_atravesar['NODO1_ID'])
                
                if otro_nodo_ec_atravesar:
                    pila_exploracion_anillo.append((ec_fid_atravesar, ec_row_atravesar['TIPO'], otro_nodo_ec_atravesar))
    
    if iter_count >= max_iteraciones_anillo:
        print(f"⚠️ Advertencia: Barrido de anillo para {co_ec_open_original} desde nodo {nodo_inicio_anillo} alcanzó el límite de iteraciones.")
    return pd.NA, pd.NA, pd.NA


def generar_dfs_resultados_finales(df_circuitos, df_elementos_corte_global, df_lineas_global, verbose=False):
    if df_circuitos is None or df_elementos_corte_global is None or df_lineas_global is None:
        print("❌ Error en la carga de datos inicial. No se puede continuar.")
        return None, None
        
    resultados_elementos_corte_acumulados_lista = []
    resultados_lineas_acumulados_lista = []

    # --- PRIMER BARRIDO DE CONECTIVIDAD ---
    print("\n🔄 Iniciando primer barrido de conectividad...")
    for _, row_circuito in df_circuitos.iterrows():
        circuito_co_inicial = str(row_circuito['Circuito'])
        print(f"  Procesando circuito (barrido inicial): {circuito_co_inicial}")

        # Filtrar elementos y líneas que pertenecen al circuito de arranque para el inicio del barrido
        # df_ecs_circuito_arranque = df_elementos_corte_global[df_elementos_corte_global['CIRCUITO'] == circuito_co_inicial].copy()
        # df_lins_circuito_arranque = df_lineas_global[df_lineas_global['CIRCUITO'] == circuito_co_inicial].copy()
        df_ecs_circuito_arranque = df_elementos_corte_global.copy()
        df_lins_circuito_arranque = df_lineas_global.copy()

        barrido_conectividad_por_circuito(
            circuito_co_inicial,
            df_ecs_circuito_arranque,
            df_lins_circuito_arranque,
            resultados_elementos_corte_acumulados_lista,
            resultados_lineas_acumulados_lista
        )
    
    df_final_elementos_corte = pd.DataFrame(resultados_elementos_corte_acumulados_lista)
    df_final_lineas = pd.DataFrame(resultados_lineas_acumulados_lista)

    # Inicializar columnas para información de anillos antes de procesarlos
    if not df_final_elementos_corte.empty:
        df_final_elementos_corte['Equipo_anillo'] = pd.NA
        df_final_elementos_corte['Elementos_Aguas_Arriba_anillo'] = pd.NA
        df_final_elementos_corte['Circuito_anillo'] = pd.NA
    else: # Si no hay elementos de corte, no hay nada que hacer para anillos
        return df_final_elementos_corte, df_final_lineas


    # --- SEGUNDO BARRIDO (ANÁLISIS DE ANILLOS PARA ECs 'OPEN') ---
    print("\n🔄 Iniciando análisis de anillos para elementos 'OPEN'...")
    if not df_final_elementos_corte.empty:
        for _, row_circuito in df_circuitos.iterrows():
            circuito_co_anillo = str(row_circuito['Circuito'])
            print(f"  Procesando circuito (barrido anillos y transferencias): {circuito_co_anillo}")
            ecs_open_para_anillo = df_final_elementos_corte[
                (df_final_elementos_corte['EST_ESTABLE'] == 'OPEN') &
                (df_final_elementos_corte['Nodo_No_Explorado_Anillo'].notna()) &
                (df_final_elementos_corte['CIRCUITO'] == circuito_co_anillo)
            ].copy()
            
            for index, row_ec_open in ecs_open_para_anillo.iterrows():
                co_ec_open = row_ec_open['CODIGO_OPERATIVO']
                nodo_explorar_anillo = row_ec_open['Nodo_No_Explorado_Anillo']
                
                if verbose: print(f"    Analizando anillo para EC 'OPEN': {co_ec_open} desde nodo {nodo_explorar_anillo}...")
                
                equipo_an, eaa_an, circ_an = barrido_anillos_especifico(
                    co_ec_open,
                    nodo_explorar_anillo,
                    df_elementos_corte_global, 
                    df_lineas_global,          
                    df_final_elementos_corte # Pasar el DF actual para consulta
                )
                
                if pd.notna(equipo_an):
                    if verbose: print(f"      Anillo encontrado para {co_ec_open}: Equipo={equipo_an}, Circuito_Anillo={circ_an}")
                    df_final_elementos_corte.loc[index, 'Equipo_anillo'] = equipo_an
                    df_final_elementos_corte.loc[index, 'Elementos_Aguas_Arriba_anillo'] = eaa_an
                    df_final_elementos_corte.loc[index, 'Circuito_anillo'] = circ_an
                else:
                    if verbose: print(f"      No se encontró conexión de anillo definida para {co_ec_open} desde nodo {nodo_explorar_anillo}.")


    # Eliminar duplicados después de todos los procesamientos
    if not df_final_elementos_corte.empty:
        cols_subset_ec = ['G3E_FID', 'Circuito_Origen_Barrido', 'Equipo_Padre', 'Elementos_Aguas_Arriba']
        cols_subset_ec_existentes = [col for col in cols_subset_ec if col in df_final_elementos_corte.columns]
        df_final_elementos_corte = df_final_elementos_corte.drop_duplicates(subset=cols_subset_ec_existentes, keep='first')
    
    if not df_final_lineas.empty:
        cols_subset_li = ['G3E_FID', 'Circuito_Origen_Barrido', 'Equipo_Padre', 'Elementos_Aguas_Arriba']
        cols_subset_li_existentes = [col for col in cols_subset_li if col in df_final_lineas.columns]
        df_final_lineas = df_final_lineas.drop_duplicates(subset=cols_subset_li_existentes, keep='first')
        
    return df_final_elementos_corte, df_final_lineas


# --- Inicio de la ejecución ---
if __name__ == "__main__":
    
    if data_load == "CSV":
        archivo_circuitos = "Data/CSV/circuitos1.csv"
        archivo_elementos_corte = "Data/CSV/elementos_corte.csv"
        archivo_lineas = "Data/CSV/Lineas.csv" 
        data_list = ["csv","csv","csv"]
    else:
        archivo_circuitos = "Data/CSV/circuitos1.csv"
        archivo_elementos_corte = "Data/SQL/elementos_corte.sql"
        archivo_lineas = "Data/SQL/Lineas.sql" 
        data_list = ["csv","oracle","oracle"]
    
    print("🔌 Iniciando proceso de barrido de conectividad eléctrica...")
    
    df_circuitos, df_ecs, df_lins = cargar_datos(
                                                file_circuitos_location = archivo_circuitos,
                                                file_elementos_corte_location = archivo_elementos_corte,
                                                file_lineas_location = archivo_lineas,
                                                source_types = data_list)

    if df_circuitos is not None and df_ecs is not None and df_lins is not None:
        
        df_resultados_ecs, df_resultados_lins = generar_dfs_resultados_finales(df_circuitos, df_ecs, df_lins)

        if df_resultados_ecs is not None and df_resultados_lins is not None:
            print("\n🎉 ¡Barridos completados (incluyendo análisis de anillos)!")
            
            if not df_resultados_ecs.empty:
                # Mostrar columnas relevantes para anillos si existen
                cols_to_show_ecs = ['CODIGO_OPERATIVO', 'EST_ESTABLE', 'Equipo_Padre', 'Circuito_Origen_Barrido', 'Nodo_No_Explorado_Anillo', 'Equipo_anillo', 'Circuito_anillo']
                cols_exist_ecs = [col for col in cols_to_show_ecs if col in df_resultados_ecs.columns]
                # df_resultados_ecs.to_excel("resultados_elementos_corte_final_con_anillos.xlsx", index=False)
            else:
                print("No se encontraron resultados para elementos de corte.")

            # ---- SECCIÓN PARA GENERAR GRAFOS ----
            if not df_resultados_ecs.empty:
                print("\n📊 Iniciando generación de grafos para elementos de corte por circuito...")
                output_folder_grafos = "grafos_circuitos_ecs" 

                if 'Circuito_Origen_Barrido' in df_resultados_ecs.columns:
                    circuitos_con_resultados = df_resultados_ecs['Circuito_Origen_Barrido'].unique()
                    
                    for circuito_actual_co in circuitos_con_resultados:
                        if pd.isna(circuito_actual_co): continue # Omitir si el circuito origen es NaN
                        df_circuito_especifico_ecs = df_resultados_ecs[
                            df_resultados_ecs['Circuito_Origen_Barrido'] == circuito_actual_co
                        ].copy()

                        if not df_circuito_especifico_ecs.empty:
                            generar_grafo_circuito(
                                df_datos_circuito=df_circuito_especifico_ecs,
                                circuito_co_origen=circuito_actual_co,
                                output_folder=output_folder_grafos,
                            )
                        else:
                            print(f"    ℹ️ No hay datos de elementos de corte para el circuito {circuito_actual_co} para generar grafo.")
                else:
                    print("    ⚠️ No se encontró la columna 'Circuito_Origen_Barrido' en los resultados de elementos de corte.")
            # ---- FIN DE LA SECCIÓN DE GRAFOS ----
        else:
            print("⚠️ No se pudieron generar los DataFrames de resultados del barrido.")
    else:
        print("❌ Error en la carga de datos. El proceso no puede continuar.")