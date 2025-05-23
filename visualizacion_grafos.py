# -*- coding: utf-8 -*-
"""
Created on Thu May 22 13:58:25 2025

@author: alopagui
"""

# visualizacion_grafos.py

import os
import graphviz
import pandas as pd

def generar_grafo_circuito(
    df_datos_circuito,
    circuito_co_origen, # CODIGO_OPERATIVO del interruptor/inicio del circuito
    output_folder,
    font_size=10,
    line_thickness=1.0,
    node_width=2.0, 
    node_height=0.6,
    rankdir='TB',
    default_node_color='lightgrey', # Color para nodos sin estado definido o tipo 'I' no raíz
    interruptor_principal_color='gold', # Color para el interruptor principal del circuito
    estado_closed_color='mediumseagreen',
    estado_open_color='tomato',
    edge_color='gray30',
    font_name='Arial'
    ):
    """
    Genera un grafo orientado para un circuito específico, con formas y colores
    basados en las columnas 'TIPO' y 'EST_ESTABLE'.
    """
    graph_name = f'Grafo_Circuito_{"".join(c if c.isalnum() else "_" for c in circuito_co_origen)}'
    dot = graphviz.Digraph(
        name=graph_name,
        comment=f'Diagrama Unifilar del Circuito {circuito_co_origen} - Elementos de Corte',
        engine='dot'
    )

    # Atributos globales del grafo
    dot.attr(rankdir=rankdir, splines='ortho', overlap='false', nodesep='0.5', ranksep='0.8')
    
    # Atributos globales base para los nodos (estilo y dimensiones)
    # La forma y el color de relleno se definirán individualmente por nodo.
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

    # Mapeo de TIPO a forma del nodo (Graphviz shapes)
    # Las claves deben estar en MAYÚSCULAS para coincidir con el procesamiento de datos
    tipo_a_forma = {
        'S': 'diamond',
        'C': 'ellipse',
        'R': 'octagon',       # Ej: Reconectador
        'I': 'doubleoctagon', # Ej: Interruptor (general)
        'P': 'parallelogram', # Ej: Protección
        'DEFAULT': 'box'      # Forma por defecto si TIPO no está en el mapeo o falta
    }
    
    # 1. Crear un diccionario de atributos para cada nodo único basado en su CODIGO_OPERATIVO
    atributos_nodos = {}
    if 'CODIGO_OPERATIVO' in df_datos_circuito.columns:
        # Usar drop_duplicates para asegurar una sola fila de atributos por CODIGO_OPERATIVO
        df_nodos_unicos_atributos = df_datos_circuito.drop_duplicates(subset=['CODIGO_OPERATIVO'])
        for _, row_nodo in df_nodos_unicos_atributos.iterrows():
            co = str(row_nodo['CODIGO_OPERATIVO'])
            atributos_nodos[co] = {
                'TIPO': str(row_nodo.get('TIPO', 'DEFAULT')).upper(), # Convertir a mayúsculas
                'EST_ESTABLE': str(row_nodo.get('EST_ESTABLE', 'DEFAULT')).upper() # Convertir a mayúsculas
            }

    # 2. Identificar todos los nodos que deben estar en el grafo (elementos y sus padres)
    nodos_a_graficar = set()
    if 'CODIGO_OPERATIVO' in df_datos_circuito.columns:
        nodos_a_graficar.update(df_datos_circuito['CODIGO_OPERATIVO'].astype(str))
    if 'Equipo_Padre' in df_datos_circuito.columns:
        # Añadir padres que podrían no estar como CODIGO_OPERATIVO principal (aunque no debería ser común)
        nodos_a_graficar.update(df_datos_circuito['Equipo_Padre'].dropna().astype(str))
    
    # Limpiar posibles 'nan' o 'none' como nombres de nodo
    nodos_a_graficar = {n for n in nodos_a_graficar if n.lower() not in ['nan', 'none']}

    # 3. Añadir nodos al grafo con su forma y color correspondientes
    for co_nodo_str in nodos_a_graficar:
        label_nodo = co_nodo_str 

        # Obtener atributos; si un nodo (ej. un padre) no está en la lista principal de COs, usar defaults.
        node_data = atributos_nodos.get(co_nodo_str, {'TIPO': 'DEFAULT', 'EST_ESTABLE': 'DEFAULT'})
        tipo_actual = node_data['TIPO']
        estado_actual = node_data['EST_ESTABLE']

        forma_nodo = tipo_a_forma.get(tipo_actual, tipo_a_forma['DEFAULT'])
        color_relleno_nodo = default_node_color # Color por defecto

        # Determinar color de relleno
        if tipo_actual == 'I' and co_nodo_str == circuito_co_origen: # Interruptor principal del circuito
            color_relleno_nodo = interruptor_principal_color
            # La forma ya está definida por tipo_a_forma['I']
        else: # Otros elementos de corte (incluidos otros interruptores no principales)
            if estado_actual == 'CLOSED':
                color_relleno_nodo = estado_closed_color
            elif estado_actual == 'OPEN':
                color_relleno_nodo = estado_open_color
            # Si no es ni CLOSED ni OPEN, se queda con default_node_color

        dot.node(name=co_nodo_str, label=label_nodo, shape=forma_nodo, fillcolor=color_relleno_nodo)

    # 4. Añadir aristas (conexiones)
    if 'Equipo_Padre' in df_datos_circuito.columns and 'CODIGO_OPERATIVO' in df_datos_circuito.columns:
        for _, row in df_datos_circuito.iterrows():
            hijo_co_str = str(row['CODIGO_OPERATIVO'])
            padre_co_str = str(row['Equipo_Padre'])

            if pd.notna(row['Equipo_Padre']) and padre_co_str.lower() != 'nan' and padre_co_str.lower() != 'none':
                # Solo añadir arista si ambos nodos existen (ya deberían por el paso anterior)
                if padre_co_str in nodos_a_graficar and hijo_co_str in nodos_a_graficar:
                    dot.edge(padre_co_str, hijo_co_str)
    
    os.makedirs(output_folder, exist_ok=True)
    
    safe_circuito_co = "".join(c if c.isalnum() or c in ('_','-') else '_' for c in circuito_co_origen)
    output_filename_base = f'circuito_ecs_{safe_circuito_co}'
    
    try:
        filepath = dot.render(filename=output_filename_base, directory=output_folder, format='svg', cleanup=True)
        print(f"✅ Grafo para circuito {circuito_co_origen} guardado en: {filepath}")
    except graphviz.backend.execute.ExecutableNotFound:
        print("❌ ERROR CRÍTICO: El ejecutable de Graphviz no se encontró.")
        print("   Por favor, instala Graphviz desde https://graphviz.org/download/ y asegúrate")
        print("   de que el directorio 'bin' de Graphviz esté en el PATH de tu sistema.")
    except Exception as e:
        print(f"❌ Error al generar o guardar el grafo para {circuito_co_origen}: {e}")