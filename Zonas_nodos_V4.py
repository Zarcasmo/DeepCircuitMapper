# -*- coding: utf-8 -*-
"""
Created on Wed May 21 17:48:23 2025

@author: alopagui
"""

# Asegúrate de tener estas importaciones al inicio de tu script
import pandas as pd
import os
import graphviz # Nueva importación

# ... (aquí va el código de las funciones cargar_datos, 
#      barrido_conectividad_por_circuito, y generar_dfs_resultados_finales
#      que te proporcioné anteriormente)
# COMIENZO DEL CÓDIGO PREVIAMENTE PROPORCIONADO (para contexto)
def cargar_datos(file_circuitos, file_elementos_corte, file_lineas):
    """
    Carga los datos desde los archivos Excel especificados.
    Convierte las columnas de NODO_ID, G3E_FID, CODIGO_OPERATIVO y Circuito a string 
    y elimina espacios en blanco al inicio/final para consistencia.
    """
    try:
        df_circuitos = pd.read_excel(file_circuitos, sheet_name="Prueba")
        df_elementos_corte = pd.read_excel(file_elementos_corte)
        df_lineas = pd.read_excel(file_lineas)
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
        pila_exploracion.append((str(elemento_arranque['NODO1_ID']), circuito_co_inicial, list(camino_co_aguas_arriba_para_hijos_de_arranque)))
    if pd.notna(elemento_arranque['NODO2_ID']) and elemento_arranque['NODO2_ID'] != 'nan':
        pila_exploracion.append((str(elemento_arranque['NODO2_ID']), circuito_co_inicial, list(camino_co_aguas_arriba_para_hijos_de_arranque)))

    while pila_exploracion:
        nodo_actual, co_ec_padre_directo, camino_co_hasta_padre_directo = pila_exploracion.pop()

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
                    pila_exploracion.append((otro_nodo_linea, co_ec_padre_directo, list(camino_co_hasta_padre_directo)))

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
                nuevo_co_ec_padre_para_hijos = str(ec_conectado_row['CODIGO_OPERATIVO'])
                nuevo_camino_co_para_hijos = list(camino_co_hasta_padre_directo) + [nuevo_co_ec_padre_para_hijos]
                if pd.notna(ec_conectado_row['NODO1_ID']) and str(ec_conectado_row['NODO1_ID']) != 'nan':
                    pila_exploracion.append((str(ec_conectado_row['NODO1_ID']), nuevo_co_ec_padre_para_hijos, nuevo_camino_co_para_hijos))
                if pd.notna(ec_conectado_row['NODO2_ID']) and str(ec_conectado_row['NODO2_ID']) != 'nan':
                    pila_exploracion.append((str(ec_conectado_row['NODO2_ID']), nuevo_co_ec_padre_para_hijos, nuevo_camino_co_para_hijos))

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
            df_elementos_corte_global,
            df_lineas_global,
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
# FIN DEL CÓDIGO PREVIAMENTE PROPORCIONADO
#---------------------------------------------------------------------

def generar_grafo_circuito(
    df_datos_circuito,
    circuito_co_origen,
    output_folder,
    font_size=10,
    line_thickness=1.0,
    node_width=2.0, 
    node_height=0.6,
    node_shape='box',
    rankdir='TB',
    node_color='lightblue',
    root_node_color='lightcoral',
    edge_color='gray30',
    font_name='Arial'
    ):
    """
    Genera un grafo orientado para un circuito específico mostrando solo elementos de corte y sus conexiones.
    Guarda el grafo como un archivo SVG en la carpeta especificada.

    Parámetros:
    - df_datos_circuito (pd.DataFrame): DataFrame filtrado con los elementos de corte del circuito.
                                        Debe contener 'CODIGO_OPERATIVO' y 'Equipo_Padre'.
    - circuito_co_origen (str): CODIGO_OPERATIVO del interruptor del circuito (para identificar el nodo raíz y nombrar el archivo).
    - output_folder (str): Carpeta donde se guardarán los grafos generados.
    - font_size (int): Tamaño de la fuente para las etiquetas de los nodos.
    - line_thickness (float): Grosor de las líneas (aristas) de conexión.
    - node_width (float): Ancho de los cuadros (nodos) en pulgadas.
    - node_height (float): Alto de los cuadros (nodos) en pulgadas.
    - node_shape (str): Forma de los nodos (ej: 'box', 'ellipse', 'circle').
    - rankdir (str): Dirección del grafo ('TB' para Top-to-Bottom, 'LR' para Left-to-Right).
    - node_color (str): Color de relleno para los nodos.
    - root_node_color (str): Color de relleno para el nodo raíz (interruptor).
    - edge_color (str): Color de las aristas.
    - font_name (str): Nombre de la fuente a utilizar.
    """
    # Crear un nombre de grafo único para evitar conflictos si se llaman múltiples veces
    # y para que el motor de Graphviz sepa que son grafos distintos.
    graph_name = f'Grafo_Circuito_{"".join(c if c.isalnum() else "_" for c in circuito_co_origen)}'

    dot = graphviz.Digraph(
        name=graph_name,
        comment=f'Diagrama Unifilar del Circuito {circuito_co_origen} - Elementos de Corte',
        engine='dot' # 'dot' es bueno para jerarquías
    )

    # Atributos globales del grafo
    dot.attr(rankdir=rankdir, splines='ortho', overlap='false', nodesep='0.5', ranksep='0.8')
    # 'splines=ortho' intenta usar líneas rectas ortogonales. 'splines=true' para curvas.
    # 'overlap=false' intenta evitar superposición de nodos.
    # 'nodesep' y 'ranksep' controlan espaciado.

    dot.attr('node', 
             shape=node_shape, 
             style='filled', 
             fillcolor=node_color, 
             fontname=font_name,
             fontsize=str(font_size),
             width=str(node_width),
             height=str(node_height),
             fixedsize='true') # 'true' para que width y height sean absolutos
    
    dot.attr('edge', 
             penwidth=str(line_thickness),
             color=edge_color,
             arrowsize='0.7')

    # Añadir nodos
    nodos_en_grafo = set() # Para evitar añadir nodos duplicados a Graphviz si aparecen varias veces
    
    # Primero, todos los CODIGO_OPERATIVO como nodos
    if 'CODIGO_OPERATIVO' in df_datos_circuito.columns:
        for co_nodo_str in df_datos_circuito['CODIGO_OPERATIVO'].astype(str).unique():
            if co_nodo_str and co_nodo_str.lower() != 'nan' and co_nodo_str.lower() != 'none':
                label_nodo = str(co_nodo_str) # El texto que se muestra en el nodo
                # Aplicar color especial al nodo raíz (interruptor del circuito)
                current_fill_color = root_node_color if co_nodo_str == circuito_co_origen else node_color
                dot.node(name=co_nodo_str, label=label_nodo, fillcolor=current_fill_color)
                nodos_en_grafo.add(co_nodo_str)

    # Asegurar que los 'Equipo_Padre' también existan como nodos si no estaban en 'CODIGO_OPERATIVO'
    # (Aunque con la lógica de barrido, todos los padres deberían ser también 'CODIGO_OPERATIVO' en alguna fila)
    if 'Equipo_Padre' in df_datos_circuito.columns:
        for co_padre_str in df_datos_circuito['Equipo_Padre'].dropna().astype(str).unique():
            if co_padre_str and co_padre_str.lower() != 'nan' and co_padre_str.lower() != 'none':
                if co_padre_str not in nodos_en_grafo:
                    label_nodo = str(co_padre_str)
                    current_fill_color = root_node_color if co_padre_str == circuito_co_origen else node_color
                    dot.node(name=co_padre_str, label=label_nodo, fillcolor=current_fill_color)
                    nodos_en_grafo.add(co_padre_str)

    # Añadir aristas (conexiones)
    if 'Equipo_Padre' in df_datos_circuito.columns and 'CODIGO_OPERATIVO' in df_datos_circuito.columns:
        for _, row in df_datos_circuito.iterrows():
            hijo_co_str = str(row['CODIGO_OPERATIVO'])
            padre_co_str = str(row['Equipo_Padre'])

            if pd.notna(row['Equipo_Padre']) and padre_co_str.lower() != 'nan' and padre_co_str.lower() != 'none':
                # Solo añadir arista si ambos nodos existen en nuestro conjunto de nodos del grafo
                if padre_co_str in nodos_en_grafo and hijo_co_str in nodos_en_grafo:
                    dot.edge(padre_co_str, hijo_co_str)
    
    os.makedirs(output_folder, exist_ok=True)
    
    # Sanitizar el nombre del circuito para usarlo en el nombre de archivo
    safe_circuito_co = "".join(c if c.isalnum() or c in ('_','-') else '_' for c in circuito_co_origen)
    output_filename_base = f'circuito_ecs_{safe_circuito_co}'
    
    try:
        # Renderizar y guardar el grafo como SVG. cleanup=True elimina el archivo fuente .gv
        # El método render devuelve la ruta completa al archivo generado.
        filepath = dot.render(filename=output_filename_base, directory=output_folder, format='svg', cleanup=True)
        print(f"✅ Grafo para circuito {circuito_co_origen} guardado en: {filepath}")
    except graphviz.backend.execute.ExecutableNotFound:
        print("❌ ERROR CRÍTICO: El ejecutable de Graphviz no se encontró.")
        print("   Por favor, instala Graphviz desde https://graphviz.org/download/ y asegúrate")
        print("   de que el directorio 'bin' de Graphviz esté en el PATH de tu sistema.")
    except Exception as e:
        print(f"❌ Error al generar o guardar el grafo para {circuito_co_origen}: {e}")


# --- Modificación de la ejecución principal ---
if __name__ == "__main__":
    archivo_circuitos = "Data/circuitos.xlsx"
    archivo_elementos_corte = "Data/elementos_corte.xlsx"
    archivo_lineas = "Data/Lineas.xlsx" # Asegúrate que el nombre coincida exactamente

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
                # df_resultados_ecs.to_excel("resultados_elementos_corte.xlsx", index=False)
                # print("\nResultados de elementos de corte guardados en 'resultados_elementos_corte.xlsx'")
            else:
                print("No se encontraron resultados para elementos de corte.")

            print("\n--- Resultados: Líneas (primeras filas) ---")
            if not df_resultados_lins.empty:
                print(df_resultados_lins.head())
                # df_resultados_lins.to_excel("resultados_lineas.xlsx", index=False)
                # print("\nResultados de líneas guardados en 'resultados_lineas.xlsx'")
            else:
                print("No se encontraron resultados para líneas.")

            # ---- NUEVA SECCIÓN PARA GENERAR GRAFOS ----
            if not df_resultados_ecs.empty:
                print("\n📊 Iniciando generación de grafos para elementos de corte por circuito...")
                output_folder_grafos = "grafos_circuitos_ecs" # Nombre de la carpeta para los grafos

                # Obtener los circuitos únicos para los que se generaron resultados
                if 'Circuito_Origen_Barrido' in df_resultados_ecs.columns:
                    circuitos_con_resultados = df_resultados_ecs['Circuito_Origen_Barrido'].unique()
                    
                    for circuito_actual_co in circuitos_con_resultados:
                        print(f"   Generando grafo para el circuito: {circuito_actual_co}...")
                        # Filtrar el DataFrame de resultados de ECs para el circuito actual
                        df_circuito_especifico_ecs = df_resultados_ecs[
                            df_resultados_ecs['Circuito_Origen_Barrido'] == circuito_actual_co
                        ].copy() # Usar .copy() para evitar SettingWithCopyWarning

                        if not df_circuito_especifico_ecs.empty:
                            generar_grafo_circuito(
                                df_datos_circuito=df_circuito_especifico_ecs,
                                circuito_co_origen=circuito_actual_co, # Este es el CO del interruptor
                                output_folder=output_folder_grafos,
                                # --- Puedes editar estos parámetros por defecto ---
                                font_size=8,          # Tamaño de la fuente en los nodos
                                line_thickness=0.8,   # Grosor de las líneas de conexión
                                node_width=1.8,       # Ancho de los cuadros (nodos) en pulgadas
                                node_height=0.4,      # Alto de los cuadros (nodos) en pulgadas
                                node_shape='box',     # Forma de los nodos: 'box', 'ellipse', 'plaintext', etc.
                                rankdir='TB',         # Dirección del grafo: 'TB' (Top-Bottom), 'LR' (Left-Right)
                                node_color='skyblue', # Color de los nodos
                                root_node_color='salmon', # Color del nodo raíz
                                edge_color='dimgray', # Color de las flechas/líneas
                                font_name='Helvetica' # Fuente (asegúrate que esté disponible)
                            )
                        else:
                            print(f"   ℹ️ No hay datos de elementos de corte para el circuito {circuito_actual_co} para generar grafo.")
                else:
                    print("   ⚠️ No se encontró la columna 'Circuito_Origen_Barrido' en los resultados de elementos de corte.")
            # ---- FIN DE LA NUEVA SECCIÓN ----
        else:
            print("⚠️ No se pudieron generar los DataFrames de resultados del barrido.")
    else:
        print("❌ Error en la carga de datos. El proceso no puede continuar.")