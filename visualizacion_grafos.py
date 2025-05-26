# -*- coding: utf-8 -*-
"""
Created on Thu May 22 13:58:25 2025

@author: alopagui
"""

import os
import graphviz
import pandas as pd

def generar_grafo_circuito(
    df_datos_circuito,
    circuito_co_origen, 
    output_folder,
    font_size=10,
    line_thickness=1.0,
    node_width=2.0, 
    node_height=0.6,
    rankdir='TB',
    default_node_color='lightgrey', 
    interruptor_principal_color='gold',
    estado_closed_color='mediumseagreen',
    estado_open_color='tomato',
    edge_color='gray30',
    font_name='Arial',
    anillo_interno_color='blue',      # Nuevo
    anillo_externo_color='darkorange', # Nuevo (era red, cambio para diferenciar de open)
    circuito_externo_node_color='lightpink' # Nuevo
    ):
    """
    Genera un grafo orientado para un circuito específico, con formas y colores
    basados en las columnas 'TIPO' y 'EST_ESTABLE', e indicando anillos.
    """
    graph_name = f'Grafo_Circuito_{"".join(c if c.isalnum() else "_" for c in str(circuito_co_origen))}'
    dot = graphviz.Digraph(
        name=graph_name,
        comment=f'Diagrama Unifilar del Circuito {circuito_co_origen} - Elementos de Corte',
        engine='dot'
    )

    dot.attr(rankdir=rankdir, splines='ortho', overlap='false', nodesep='0.5', ranksep='0.8')
    
    dot.attr('node', 
             style='filled', 
             fontname=font_name,
             fontsize=str(font_size),
             width=str(node_width),
             height=str(node_height),
             fixedsize='true')
    
    dot.attr('edge', 
             penwidth=str(line_thickness),
             color=edge_color,
             arrowsize='0.7')

    tipo_a_forma = {
        'S': 'diamond',
        'C': 'ellipse',
        'R': 'octagon',    
        'I': 'doubleoctagon',
        'P': 'parallelogram',
        'DEFAULT': 'box'    
    }
    
    atributos_nodos = {}
    if 'CODIGO_OPERATIVO' in df_datos_circuito.columns:
        df_nodos_unicos_atributos = df_datos_circuito.drop_duplicates(subset=['CODIGO_OPERATIVO'])
        for _, row_nodo in df_nodos_unicos_atributos.iterrows():
            co = str(row_nodo['CODIGO_OPERATIVO'])
            atributos_nodos[co] = {
                'TIPO': str(row_nodo.get('TIPO', 'DEFAULT')).upper(),
                'EST_ESTABLE': str(row_nodo.get('EST_ESTABLE', 'DEFAULT')).upper()
            }

    nodos_a_graficar = set()
    if 'CODIGO_OPERATIVO' in df_datos_circuito.columns:
        nodos_a_graficar.update(df_datos_circuito['CODIGO_OPERATIVO'].astype(str))
    if 'Equipo_Padre' in df_datos_circuito.columns:
        nodos_a_graficar.update(df_datos_circuito['Equipo_Padre'].dropna().astype(str))
    
    nodos_a_graficar = {n for n in nodos_a_graficar if n.lower() not in ['nan', 'none', 'pd.na', '<na>']}


    for co_nodo_str in nodos_a_graficar:
        if pd.isna(co_nodo_str) or str(co_nodo_str).strip() == '': continue # Saltar nodos vacíos o NaN
        label_nodo = co_nodo_str 

        node_data = atributos_nodos.get(co_nodo_str, {'TIPO': 'DEFAULT', 'EST_ESTABLE': 'DEFAULT'})
        tipo_actual = node_data['TIPO']
        estado_actual = node_data['EST_ESTABLE'] # Ya está en mayúsculas desde la carga

        forma_nodo = tipo_a_forma.get(tipo_actual, tipo_a_forma['DEFAULT'])
        color_relleno_nodo = default_node_color

        if tipo_actual == 'I' and co_nodo_str == str(circuito_co_origen):
            color_relleno_nodo = interruptor_principal_color
        else:
            if estado_actual == 'CLOSED':
                color_relleno_nodo = estado_closed_color
            elif estado_actual == 'OPEN':
                color_relleno_nodo = estado_open_color
        
        dot.node(name=co_nodo_str, label=label_nodo, shape=forma_nodo, fillcolor=color_relleno_nodo)

    # --- Añadir aristas (conexiones principales) ---
    if 'Equipo_Padre' in df_datos_circuito.columns and 'CODIGO_OPERATIVO' in df_datos_circuito.columns:
        for _, row in df_datos_circuito.iterrows():
            hijo_co_str = str(row['CODIGO_OPERATIVO'])
            padre_co_str = str(row['Equipo_Padre'])

            if pd.notna(row['Equipo_Padre']) and padre_co_str.lower() not in ['nan', 'none', 'pd.na', '<na>'] and \
               hijo_co_str.lower() not in ['nan', 'none', 'pd.na', '<na>']:
                if padre_co_str in nodos_a_graficar and hijo_co_str in nodos_a_graficar:
                    dot.edge(padre_co_str, hijo_co_str)
    
    # --- Añadir aristas para ANILLOS (desde ECs 'OPEN') ---
    nodos_externos_creados = set() # Para no duplicar nodos de circuitos externos
    if all(col in df_datos_circuito.columns for col in ['CODIGO_OPERATIVO', 'EST_ESTABLE', 'Equipo_anillo', 'Circuito_anillo', 'Circuito_Origen_Barrido']):
        ecs_open_con_anillo = df_datos_circuito[
            (df_datos_circuito['EST_ESTABLE'] == 'OPEN') &
            (df_datos_circuito['Equipo_anillo'].notna())
        ]

        for _, row_nodo_open in ecs_open_con_anillo.iterrows():
            co_ec_open = str(row_nodo_open['CODIGO_OPERATIVO'])
            equipo_anillo_co = str(row_nodo_open['Equipo_anillo'])
            circuito_anillo_val = str(row_nodo_open.get('Circuito_anillo', 'N/A'))
            # circuito_origen_actual = str(row_nodo_open.get('Circuito_Origen_Barrido', circuito_co_origen))
            # Usar el circuito_co_origen de la función es más seguro para el contexto del grafo actual
            circuito_origen_actual_grafo = str(circuito_co_origen)


            if co_ec_open not in nodos_a_graficar: continue # El EC open debe existir

            # Anillo Interno
            if circuito_anillo_val == circuito_origen_actual_grafo and equipo_anillo_co in nodos_a_graficar:
                dot.edge(co_ec_open, equipo_anillo_co, 
                         color=anillo_interno_color, 
                         style='dashed', arrowhead='normal', constraint='false')
                         #label=f"Anillo interno\na {equipo_anillo_co[:15]}") # Etiqueta opcional
            # Anillo Externo
            elif circuito_anillo_val != circuito_origen_actual_grafo and pd.notna(row_nodo_open['Equipo_anillo']):
                nodo_circuito_externo_label = f"Circuito:\n{circuito_anillo_val}"
                nodo_circuito_externo_name = f"ext_circ_{''.join(c if c.isalnum() else '_' for c in circuito_anillo_val)}"              

                if nodo_circuito_externo_name not in nodos_externos_creados:
                    dot.node(name=nodo_circuito_externo_name, 
                             label=nodo_circuito_externo_label, 
                             shape='box', style='filled,dashed', 
                             fillcolor=circuito_externo_node_color,
                             color=anillo_externo_color) # Borde del color de la línea
                    nodos_externos_creados.add(nodo_circuito_externo_name)
                
                dot.edge(co_ec_open, nodo_circuito_externo_name, 
                         color=anillo_externo_color, 
                         style='dashed', arrowhead='normal', constraint='false')
                         #label=f"Anillo a Cto.\n{circuito_anillo_val}") # Etiqueta opcional
        
    os.makedirs(output_folder, exist_ok=True)
    
    safe_circuito_co = "".join(c if c.isalnum() or c in ('_','-') else '_' for c in str(circuito_co_origen))
    output_filename_base = f'circuito_ecs_{safe_circuito_co}'
    
    try:
        filepath = dot.render(filename=output_filename_base, directory=output_folder, format='svg', cleanup=True)
        print(f"✅ Grafo para circuito {circuito_co_origen} guardado en: {filepath}")
    except graphviz.backend.execute.ExecutableNotFound:
        print("❌ ERROR CRÍTICO: El ejecutable de Graphviz no se encontró.")
        print("    Por favor, instala Graphviz desde https://graphviz.org/download/ y asegúrate")
        print("    de que el directorio 'bin' de Graphviz esté en el PATH de tu sistema.")
    except Exception as e:
        print(f"❌ Error al generar o guardar el grafo para {circuito_co_origen}: {e}")