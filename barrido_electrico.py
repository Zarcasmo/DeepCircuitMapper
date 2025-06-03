import pandas as pd
import sys # Necesario para la barra de progreso
import time # Necesario para la barra de progreso

# ---- Dependencias de la Barra de Progreso ----
try:
    from colorama import Fore, Style, init
    init(autoreset=True) # Inicializa colorama para que los estilos se reseteen después de cada print
except ImportError:
    # Fallback si colorama no está instalado para que el script no falle
    class Fore: #type: ignore
        GREEN = ""; CYAN = ""; RESET = ""
    class Style: #type: ignore
        RESET_ALL = ""
    print("⚠️ Advertencia: La librería 'colorama' no está instalada. La barra de progreso no tendrá colores.")

# ---- Importar dependencias del proyecto ----
from visualizacion_grafos import generar_grafo_circuito
from Data_process import cargar_datos # Asumo que Data_process.py está en tu PYTHONPATH o mismo directorio
# ------------------------------------

data_load = "oracle" # O "CSV" según tu configuración

# ----------------------------------------------------------
# Función de Barra de Progreso (modificada ligeramente para prefijo y robustez)
# ----------------------------------------------------------
def print_progress_bar(iteration, total, prefix='', suffix='Completado', length=30, start_time=None):
    """
    Imprime una barra de progreso en la consola y muestra el tiempo transcurrido al completar.

    :param iteration: El número actual de iteraciones (1-based).
    :param total: El número total de iteraciones.
    :param prefix: Texto a mostrar antes de la barra de progreso.
    :param suffix: Texto a mostrar después del porcentaje.
    :param length: La longitud de la barra de progreso.
    :param start_time: El tiempo de inicio de la ejecución.
    """
    if total == 0: # Evitar división por cero si no hay iteraciones
        progress = 1.0
    else:
        progress = (iteration / total)
    
    filled_length = int(round(progress * length))
    arrow = '█' * filled_length
    spaces = '░' * (length - filled_length)
    percent = int(round(progress * 100))
    
    # Usar colores si colorama está disponible
    bar_color = Fore.GREEN
    percent_color = Fore.CYAN
    reset_color = Style.RESET_ALL

    bar = f'[{bar_color}{arrow}{spaces}{reset_color}]'
    percent_text = f'{percent_color}{percent}%{reset_color}'
    
    time_str = ""
    if start_time:
        elapsed_time = time.time() - start_time
        elapsed_time_formatted = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        time_str = f' - Transcurrido: {elapsed_time_formatted}'
    
    # \r para volver al inicio de la línea. Espacios al final para limpiar caracteres residuales.
    sys.stdout.write(f'\r{prefix} {bar} {percent_text} {suffix}{time_str}      ') 
    
    if iteration == total and total > 0: # Nueva línea cuando se completa el total (y si hubo progreso)
        sys.stdout.write('\n')
    
    sys.stdout.flush()

# ----------------------------------------------------------
# Código de Barrido (funciones existentes)
# ----------------------------------------------------------
def barrido_conectividad_por_circuito(
    circuito_co_inicial,
    df_elementos_corte_circuito, 
    df_lineas_circuito,         
    df_trafos_circuito,          
    resultados_elementos_corte_global_lista,
    resultados_lineas_global_lista,
    resultados_transformadores_global_lista 
    ):
    elementos_arranque = df_elementos_corte_circuito[df_elementos_corte_circuito['CODIGO_OPERATIVO'] == circuito_co_inicial]
    if elementos_arranque.empty:
        # Ya no se imprime aquí, se maneja con la barra o al final.
        # print(f"ℹ️ Advertencia: No se encontró el elemento de arranque con CODIGO_OPERATIVO '{circuito_co_inicial}' en los elementos del circuito.")
        return

    elemento_arranque = elementos_arranque.iloc[0].copy()
    fid_arranque = str(elemento_arranque['G3E_FID'])

    visitados_ec_fids_este_circuito = set()
    visitados_lineas_fids_este_circuito = set()
    visitados_trafos_fids_este_circuito = set() 

    elemento_arranque_dict = elemento_arranque.to_dict()
    elemento_arranque_dict['Equipo_Padre'] = None 
    elemento_arranque_dict['Elementos_Aguas_Arriba'] = circuito_co_inicial
    elemento_arranque_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
    elemento_arranque_dict['Nodo_No_Explorado_Anillo'] = pd.NA 
    resultados_elementos_corte_global_lista.append(elemento_arranque_dict)
    visitados_ec_fids_este_circuito.add(fid_arranque)

    pila_exploracion = []
    camino_co_aguas_arriba_para_hijos_de_arranque = [circuito_co_inicial]

    if pd.notna(elemento_arranque['NODO2_ID']) and str(elemento_arranque['NODO2_ID']).lower() != 'nan':
        pila_exploracion.append((str(elemento_arranque['G3E_FID']), elemento_arranque['TIPO'], str(elemento_arranque['NODO2_ID']), circuito_co_inicial, list(camino_co_aguas_arriba_para_hijos_de_arranque)))

    while pila_exploracion:
        fid_actual, tipo_actual, nodo_actual, co_ec_padre_directo, camino_co_hasta_padre_directo = pila_exploracion.pop()

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
                elementos_aguas_arriba_linea = ",".join(camino_co_hasta_padre_directo)
                linea_dict['Elementos_Aguas_Arriba'] = elementos_aguas_arriba_linea
                linea_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
                resultados_lineas_global_lista.append(linea_dict)
                
                otro_nodo_linea = None
                if str(linea_conectada_row['NODO1_ID']) == nodo_actual and pd.notna(linea_conectada_row['NODO2_ID']) and str(linea_conectada_row['NODO2_ID']).lower() != 'nan':
                    otro_nodo_linea = str(linea_conectada_row['NODO2_ID'])
                elif str(linea_conectada_row['NODO2_ID']) == nodo_actual and pd.notna(linea_conectada_row['NODO1_ID']) and str(linea_conectada_row['NODO1_ID']).lower() != 'nan':
                    otro_nodo_linea = str(linea_conectada_row['NODO1_ID'])
                
                nodos_de_la_linea_actual = set()
                if pd.notna(nodo_actual) and str(nodo_actual).lower() != 'nan':
                    nodos_de_la_linea_actual.add(nodo_actual)
                if pd.notna(otro_nodo_linea) and str(otro_nodo_linea).lower() != 'nan':
                    nodos_de_la_linea_actual.add(otro_nodo_linea)

                for nodo_en_linea in nodos_de_la_linea_actual:
                    trafos_encontrados_en_nodo = df_trafos_circuito[
                        (df_trafos_circuito['NODO1_ID'] == nodo_en_linea) | \
                        (df_trafos_circuito['NODO2_ID'] == nodo_en_linea)
                    ]
                    for _, trafo_row_original in trafos_encontrados_en_nodo.iterrows():
                        trafo_row = trafo_row_original.copy()
                        trafo_fid = str(trafo_row['G3E_FID'])
                        if trafo_fid not in visitados_trafos_fids_este_circuito:
                            visitados_trafos_fids_este_circuito.add(trafo_fid)
                            trafo_dict = trafo_row.to_dict()
                            trafo_dict['Linea_Conexion_FID'] = linea_fid
                            trafo_dict['Elementos_Aguas_Arriba'] = elementos_aguas_arriba_linea
                            trafo_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
                            trafo_dict['Equipo_Padre_Linea'] = co_ec_padre_directo
                            resultados_transformadores_global_lista.append(trafo_dict)
                                
                if otro_nodo_linea: 
                    pila_exploracion.append((linea_fid, 'LINEA', otro_nodo_linea, co_ec_padre_directo, list(camino_co_hasta_padre_directo)))

        ecs_conectados = df_elementos_corte_circuito[
            ((df_elementos_corte_circuito['NODO1_ID'] == nodo_actual) | (df_elementos_corte_circuito['NODO2_ID'] == nodo_actual)) &
            (df_elementos_corte_circuito['G3E_FID'] != fid_actual) 
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
                ec_dict['Nodo_No_Explorado_Anillo'] = pd.NA 

                if ('CIRCUITO' in ec_conectado_row and ec_conectado_row['CIRCUITO'] != circuito_co_inicial):
                    ec_conectado_row['EST_ESTABLE'] = 'OPEN' 
                    ec_dict['EST_ESTABLE_ORIGINAL'] = ec_conectado_row_original['EST_ESTABLE'] 
                    ec_dict['EST_ESTABLE'] = 'OPEN' 
                
                nodo_no_explorado_para_anillo = pd.NA
                if ec_conectado_row['EST_ESTABLE'] == 'OPEN': 
                    if str(ec_conectado_row['NODO1_ID']) == nodo_actual and pd.notna(ec_conectado_row['NODO2_ID']) and str(ec_conectado_row['NODO2_ID']).lower() != 'nan':
                        nodo_no_explorado_para_anillo = str(ec_conectado_row['NODO2_ID'])
                    elif str(ec_conectado_row['NODO2_ID']) == nodo_actual and pd.notna(ec_conectado_row['NODO1_ID']) and str(ec_conectado_row['NODO1_ID']).lower() != 'nan':
                        nodo_no_explorado_para_anillo = str(ec_conectado_row['NODO1_ID'])
                    ec_dict['Nodo_No_Explorado_Anillo'] = nodo_no_explorado_para_anillo
                
                resultados_elementos_corte_global_lista.append(ec_dict)

                if ec_conectado_row['EST_ESTABLE'] == 'CLOSED': 
                    nuevo_co_ec_padre_para_hijos = str(ec_conectado_row['CODIGO_OPERATIVO'])
                    nuevo_camino_co_para_hijos = list(camino_co_hasta_padre_directo) + [nuevo_co_ec_padre_para_hijos]
                    
                    otro_nodo_ec_para_explorar = pd.NA
                    if str(ec_conectado_row['NODO1_ID']) == nodo_actual and pd.notna(ec_conectado_row['NODO2_ID']) and str(ec_conectado_row['NODO2_ID']).lower() != 'nan':
                        otro_nodo_ec_para_explorar = str(ec_conectado_row['NODO2_ID'])
                    elif str(ec_conectado_row['NODO2_ID']) == nodo_actual and pd.notna(ec_conectado_row['NODO1_ID']) and str(ec_conectado_row['NODO1_ID']).lower() != 'nan':
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
    pila_exploracion_anillo = []
    visitados_fids_este_anillo = set() 
    visitados_fids_este_anillo.add(co_ec_open_original) 

    pila_exploracion_anillo.append( (None, "NODO_INICIAL_ANILLO", nodo_inicio_anillo) )
    max_iteraciones_anillo = 100 
    iter_count = 0

    while pila_exploracion_anillo and iter_count < max_iteraciones_anillo:
        iter_count += 1
        fid_padre_actual, tipo_padre_actual, nodo_actual = pila_exploracion_anillo.pop()

        ecs_conectados_directo_anillo = df_elementos_corte_global[
            ((df_elementos_corte_global['NODO1_ID'] == nodo_actual) | (df_elementos_corte_global['NODO2_ID'] == nodo_actual)) &
            (df_elementos_corte_global['CODIGO_OPERATIVO'] != co_ec_open_original) 
        ]
        for _, ec_row in ecs_conectados_directo_anillo.iterrows():
            co_ec_encontrado = str(ec_row['CODIGO_OPERATIVO'])
            info_ec_encontrado_en_barrido1 = df_resultados_ecs_completos[
                df_resultados_ecs_completos['CODIGO_OPERATIVO'] == co_ec_encontrado
            ]
            if not info_ec_encontrado_en_barrido1.empty:
                data_ec_encontrado = info_ec_encontrado_en_barrido1.iloc[0]
                elementos_aguas_arriba_anillo = data_ec_encontrado.get('Elementos_Aguas_Arriba', pd.NA)
                circuito_origen_anillo = data_ec_encontrado.get('Circuito_Origen_Barrido', pd.NA)
                return co_ec_encontrado, elementos_aguas_arriba_anillo, circuito_origen_anillo
            else: 
                return co_ec_encontrado, pd.NA, pd.NA 

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
        
        ecs_conectados_para_atravesar = df_elementos_corte_global[
            ((df_elementos_corte_global['NODO1_ID'] == nodo_actual) | (df_elementos_corte_global['NODO2_ID'] == nodo_actual)) &
            (df_elementos_corte_global['CODIGO_OPERATIVO'] != co_ec_open_original) &
            (df_elementos_corte_global['EST_ESTABLE'] == 'CLOSED') 
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
    
    # if iter_count >= max_iteraciones_anillo:
        # print(f"⚠️ Advertencia: Barrido de anillo para {co_ec_open_original} desde nodo {nodo_inicio_anillo} alcanzó el límite de iteraciones.")
    return pd.NA, pd.NA, pd.NA


def generar_dfs_resultados_finales(df_circuitos, df_elementos_corte_global, df_lineas_global, df_trafos_global, verbose=False):
    if df_circuitos is None or df_elementos_corte_global is None or df_lineas_global is None or df_trafos_global is None:
        print("❌ Error en la carga de datos inicial. No se puede continuar.")
        # Devolver tuplas vacías con la nueva estructura esperada
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame() 
        
    resultados_elementos_corte_acumulados_lista = []
    resultados_lineas_acumulados_lista = []
    resultados_transformadores_acumulados_lista = []

    # --- PRIMER BARRIDO DE CONECTIVIDAD ---
    print("\n🔄 Iniciando primer barrido de conectividad")
    total_circuitos_barrido1 = len(df_circuitos)
    start_time_barrido1 = time.time()
    if total_circuitos_barrido1 > 0:
        print_progress_bar(0, total_circuitos_barrido1, prefix='Barrido Principal:', start_time=start_time_barrido1)
    
    for i, (_, row_circuito) in enumerate(df_circuitos.iterrows()):
        circuito_co_inicial = str(row_circuito['Circuito'])
        # El print individual del circuito puede eliminarse o hacerse condicional si interfiere con la barra
        # print(f"  Procesando circuito (barrido inicial): {circuito_co_inicial}")

        # Usar copias de los dataframes globales para asegurar que no se modifican entre iteraciones de circuito
        # La lógica original parecía pasar los globales directamente, lo cual es más eficiente si
        # barrido_conectividad_por_circuito no los modifica (y no debería).
        # Aquí, df_ecs_circuito_arranque, etc., son los DataFrames globales completos.
        # El filtrado por 'CIRCUITO' dentro de barrido_conectividad_por_circuito es para el elemento de arranque.
        df_ecs_para_barrido = df_elementos_corte_global 
        df_lins_para_barrido = df_lineas_global
        df_trafos_para_barrido = df_trafos_global

        barrido_conectividad_por_circuito(
            circuito_co_inicial,
            df_ecs_para_barrido, # Se usa para encontrar el arranque, luego el barrido usa este mismo para buscar
            df_lins_para_barrido,
            df_trafos_para_barrido,
            resultados_elementos_corte_acumulados_lista,
            resultados_lineas_acumulados_lista,
            resultados_transformadores_acumulados_lista
        )
        if total_circuitos_barrido1 > 0:
            print_progress_bar(i + 1, total_circuitos_barrido1, prefix='Barrido Principal:', start_time=start_time_barrido1)
    
    df_final_elementos_corte = pd.DataFrame(resultados_elementos_corte_acumulados_lista)
    df_final_lineas = pd.DataFrame(resultados_lineas_acumulados_lista)
    df_final_trafos = pd.DataFrame(resultados_transformadores_acumulados_lista) # Crear DataFrame de trafos

    if not df_final_elementos_corte.empty:
        df_final_elementos_corte['Equipo_anillo'] = pd.NA
        df_final_elementos_corte['Elementos_Aguas_Arriba_anillo'] = pd.NA
        df_final_elementos_corte['Circuito_anillo'] = pd.NA
    else:
        # Si no hay elementos de corte, df_final_lineas y df_final_trafos podrían aún tener datos
        # pero el análisis de anillos no procederá.
        print("ℹ️ No se encontraron elementos de corte en el primer barrido. Análisis de anillos omitido.")
        # Asegurarse de devolver los 3 DataFrames
        return df_final_elementos_corte, df_final_lineas, df_final_trafos

    # --- SEGUNDO BARRIDO (ANÁLISIS DE ANILLOS PARA ECs 'OPEN') ---
    print("\n🔄 Iniciando análisis de anillos y transferencias")
    total_circuitos_barrido2 = len(df_circuitos) # La barra es por circuito procesado para anillos
    start_time_barrido2 = time.time()
    if total_circuitos_barrido2 > 0:
        print_progress_bar(0, total_circuitos_barrido2, prefix='Análisis Anillos:', start_time=start_time_barrido2)

    for i, (_, row_circuito) in enumerate(df_circuitos.iterrows()):
        circuito_co_anillo_iter = str(row_circuito['Circuito'])
        # print(f"  Procesando circuito para anillos: {circuito_co_anillo_iter}")

        # Filtrar ECs 'OPEN' que pertenecen al circuito actual de esta iteración del bucle de anillos
        # La columna 'CIRCUITO' en df_final_elementos_corte debe existir y ser la del EC original.
        if 'CIRCUITO' in df_final_elementos_corte.columns:
            ecs_open_para_anillo_en_circuito_actual = df_final_elementos_corte[
                (df_final_elementos_corte['EST_ESTABLE'] == 'OPEN') &
                (df_final_elementos_corte['Nodo_No_Explorado_Anillo'].notna()) &
                (df_final_elementos_corte['CIRCUITO'] == circuito_co_anillo_iter) # ECs del circuito actual
            ].copy()
        else:
            if verbose: print(f"⚠️ Advertencia: Columna 'CIRCUITO' no encontrada en df_final_elementos_corte al procesar anillos para {circuito_co_anillo_iter}.")
            ecs_open_para_anillo_en_circuito_actual = pd.DataFrame() # Vacío para no procesar


        for index, row_ec_open in ecs_open_para_anillo_en_circuito_actual.iterrows():
            co_ec_open = row_ec_open['CODIGO_OPERATIVO']
            nodo_explorar_anillo = row_ec_open['Nodo_No_Explorado_Anillo']
            
            # El print detallado aquí puede ser controlado por 'verbose'
            if verbose: print(f"    Analizando anillo para EC 'OPEN': {co_ec_open} (Cto: {circuito_co_anillo_iter}) desde nodo {nodo_explorar_anillo}...")
            
            equipo_an, eaa_an, circ_an = barrido_anillos_especifico(
                co_ec_open,
                nodo_explorar_anillo,
                df_elementos_corte_global, 
                df_lineas_global,         
                df_final_elementos_corte 
            )
            
            if pd.notna(equipo_an):
                if verbose: print(f"      Anillo encontrado para {co_ec_open}: Equipo={equipo_an}, Circuito_Anillo={circ_an}")
                df_final_elementos_corte.loc[index, 'Equipo_anillo'] = equipo_an
                df_final_elementos_corte.loc[index, 'Elementos_Aguas_Arriba_anillo'] = eaa_an
                df_final_elementos_corte.loc[index, 'Circuito_anillo'] = circ_an
            # else:
                # if verbose: print(f"      No se encontró conexión de anillo definida para {co_ec_open} desde nodo {nodo_explorar_anillo}.")
        
        if total_circuitos_barrido2 > 0:
            print_progress_bar(i + 1, total_circuitos_barrido2, prefix='Análisis Anillos:', start_time=start_time_barrido2)
            
    # Eliminar duplicados después de todos los procesamientos
    if not df_final_elementos_corte.empty:
        cols_subset_ec = ['G3E_FID', 'Circuito_Origen_Barrido', 'Equipo_Padre', 'Elementos_Aguas_Arriba']
        # Asegurarse que las columnas para dropear duplicados realmente existen
        cols_subset_ec_existentes = [col for col in cols_subset_ec if col in df_final_elementos_corte.columns]
        if cols_subset_ec_existentes: # Solo si hay columnas para subset
             df_final_elementos_corte = df_final_elementos_corte.drop_duplicates(subset=cols_subset_ec_existentes, keep='first')
    
    if not df_final_lineas.empty:
        cols_subset_li = ['G3E_FID', 'Circuito_Origen_Barrido', 'Equipo_Padre', 'Elementos_Aguas_Arriba']
        cols_subset_li_existentes = [col for col in cols_subset_li if col in df_final_lineas.columns]
        if cols_subset_li_existentes:
            df_final_lineas = df_final_lineas.drop_duplicates(subset=cols_subset_li_existentes, keep='first')
        
    return df_final_elementos_corte, df_final_lineas, df_final_trafos


# --- Inicio de la ejecución ---
if __name__ == "__main__":
    start_total_time = time.time() # Tiempo total de ejecución del script
    
    # Configuración de carga de datos (como la tenías)
    if data_load == "CSV":
        archivo_circuitos = "Data/CSV/circuitos.csv"
        archivo_elementos_corte = "Data/CSV/elementos_corte.csv"
        archivo_lineas = "Data/CSV/Lineas.csv"
        archivo_trafos = "Data/CSV/transformadores.csv"
        data_list_source_types = ["csv","csv","csv","csv"]
    else: # Asumiendo "oracle"
        archivo_circuitos = "Data/CSV/circuitos.csv" # Este parece seguir siendo CSV
        archivo_elementos_corte = "Data/SQL/elementos_corte.sql"
        archivo_lineas = "Data/SQL/Lineas.sql" 
        archivo_trafos = "Data/SQL/transformadores.sql"
        data_list_source_types = ["csv","oracle","oracle","oracle"]
    
    print("🔌 Proceso de barrido iterativo de conectividad eléctrica - EDEQ ")
    
    df_circuitos, df_ecs, df_lins, df_trafos = cargar_datos(
        file_circuitos_location = archivo_circuitos,
        file_elementos_corte_location = archivo_elementos_corte,
        file_lineas_location = archivo_lineas,
        file_trafos_location = archivo_trafos, # Nueva entrada para trafos
        source_types = data_list_source_types
    )

    if df_circuitos is not None and df_ecs is not None and df_lins is not None and df_trafos is not None:
        print("✅ Datos cargados exitosamente.")
        
        # Pasar True para verbose si quieres ver los prints detallados del análisis de anillos
        df_resultados_ecs, df_resultados_lins, df_resultados_trafos = generar_dfs_resultados_finales(
            df_circuitos, df_ecs, df_lins, df_trafos, verbose=False 
        )

        if df_resultados_ecs is not None and df_resultados_lins is not None and df_resultados_trafos is not None:
            print("\n🎉 ¡Barridos completados (incluyendo análisis de anillos y trafos)!")
            
            # Guardar resultados en Excel (opcional)
            # print("\n💾 Guardando resultados en archivos Excel...")
            # try:
            #     with pd.ExcelWriter("resultados_barrido_electrico.xlsx") as writer:
            #         if not df_resultados_ecs.empty:
            #             df_resultados_ecs.to_excel(writer, sheet_name='Elementos_Corte', index=False)
            #         if not df_resultados_lins.empty:
            #             df_resultados_lins.to_excel(writer, sheet_name='Lineas', index=False)
            #         if not df_resultados_trafos.empty:
            #             df_resultados_trafos.to_excel(writer, sheet_name='Transformadores', index=False)
            #     print("✅ Resultados guardados en 'resultados_barrido_electrico.xlsx'")
            # except Exception as e:
            #     print(f"❌ Error al guardar resultados en Excel: {e}")

            # ---- SECCIÓN PARA GENERAR GRAFOS ----
            if not df_resultados_ecs.empty:
                print("\n📊 Iniciando generación de grafos para elementos de corte por circuito...")
                output_folder_grafos = "grafos_circuitos_ecs" 

                if 'Circuito_Origen_Barrido' in df_resultados_ecs.columns:
                    circuitos_con_resultados_unicos = df_resultados_ecs['Circuito_Origen_Barrido'].dropna().unique()
                    
                    total_grafos = len(circuitos_con_resultados_unicos)
                    start_time_grafos = time.time()
                    if total_grafos > 0:
                         print_progress_bar(0, total_grafos, prefix='Generando Grafos:', start_time=start_time_grafos)

                    for i, circuito_actual_co in enumerate(circuitos_con_resultados_unicos):
                        # print(f"    Generando grafo para el circuito: {circuito_actual_co}...") # Interfiere con la barra
                        df_circuito_especifico_ecs = df_resultados_ecs[
                            df_resultados_ecs['Circuito_Origen_Barrido'] == circuito_actual_co
                        ].copy()

                        if not df_circuito_especifico_ecs.empty:
                            generar_grafo_circuito( # Asumo que esta función viene de visualizacion_grafos.py
                                df_datos_circuito=df_circuito_especifico_ecs,
                                circuito_co_origen=circuito_actual_co,
                                output_folder=output_folder_grafos
                                # Aquí puedes añadir los parámetros de personalización de colores de anillos si es necesario
                            )
                        # else:
                            # print(f"    ℹ️ No hay datos de ECs para el circuito {circuito_actual_co} para generar grafo.")
                        if total_grafos > 0:
                            print_progress_bar(i + 1, total_grafos, prefix='Generando Grafos:', start_time=start_time_grafos)
                else:
                    print("    ⚠️ No se encontró la columna 'Circuito_Origen_Barrido' en los resultados de ECs para generar grafos.")
            # ---- FIN DE LA SECCIÓN DE GRAFOS ----
        else:
            print("⚠️ No se pudieron generar los DataFrames de resultados del barrido.")
    else:
        print("❌ Error en la carga de datos. El proceso no puede continuar.")

    end_total_time = time.time()
    total_execution_time = time.strftime("%H:%M:%S", time.gmtime(end_total_time - start_total_time))
    print(f"\n🏁 Proceso completo. Tiempo total de ejecución: {total_execution_time}")