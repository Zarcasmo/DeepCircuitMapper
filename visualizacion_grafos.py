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
    df_datos_circuito_ecs, # DataFrame con elementos de corte, incluyendo info de anillos
    # df_datos_lineas, # Podría ser necesario para trazar el camino del anillo si incluye líneas
    circuito_co_origen,
    output_folder,
    font_size=8,
    line_thickness=0.8,
    node_width=1.9, 
    node_height=0.5,
    rankdir='TB',
    default_node_color='lightsteelblue',
    interruptor_principal_color='gold',
    estado_closed_color='mediumseagreen',
    estado_open_color='tomato',
    anillo_interno_color='blue',
    anillo_externo_color='purple',
    camino_anillo_color='darkorange', # Color para elementos en el camino del anillo
    edge_color='dimgray',
    font_name='Helvetica'
    ):
    
    graph_name = f'Grafo_Circuito_{"".join(c if c.isalnum() else "_" for c in circuito_co_origen)}'
    dot = graphviz.Digraph(name=graph_name, comment=f'Circuito {circuito_co_origen}', engine='dot')
    dot.attr(rankdir=rankdir, splines='polyline', overlap='false', nodesep='0.6', ranksep='1.0') # polyline para mejor control de aristas
    dot.attr('node', style='filled', fontname=font_name, fontsize=str(font_size),
             width=str(node_width), height=str(node_height), fixedsize='true')
    dot.attr('edge', penwidth=str(line_thickness), color=edge_color, arrowsize='0.6')

    tipo_a_forma = {'S': 'diamond', 'C': 'ellipse', 'R': 'octagon', 'I': 'doubleoctagon', 'P': 'parallelogram', 'DEFAULT': 'box'}
    
    # --- Nodos Principales y del Camino del Anillo ---
    nodos_en_grafo = set()
    # Crear un lookup para atributos más fácil
    atributos_nodos = {}
    # Iterar sobre df_datos_circuito_ecs para poblar atributos_nodos
    # Asegurarse que TIPO y EST_ESTABLE están en mayúsculas para los lookups
    for _, row in df_datos_circuito_ecs.iterrows():
        co = str(row['CODIGO_OPERATIVO'])
        atributos_nodos[co] = {
            'TIPO': str(row.get('TIPO', 'DEFAULT')).upper(),
            'EST_ESTABLE': str(row.get('EST_ESTABLE', 'DEFAULT')).upper(),
            'Equipo_Padre': str(row.get('Equipo_Padre', 'NAN')).upper(),
            'Es_Parte_De_Anillo': row.get('Es_Parte_De_Anillo', False), # Nueva info
            'Anillo_Tipo': str(row.get('Anillo_Tipo', 'NAN')).upper(),
            'Anillo_Con_Elemento_CO': str(row.get('Anillo_Con_Elemento_CO', 'NAN')).upper(),
            'Anillo_Con_Circuito_CO': str(row.get('Anillo_Con_Circuito_CO', 'NAN')).upper()
        }
        nodos_en_grafo.add(co)
        if atributos_nodos[co]['Equipo_Padre'] != 'NAN':
            nodos_en_grafo.add(atributos_nodos[co]['Equipo_Padre'])
        if atributos_nodos[co]['Anillo_Con_Elemento_CO'] != 'NAN':
            nodos_en_grafo.add(atributos_nodos[co]['Anillo_Con_Elemento_CO'])


    for co_nodo in sorted(list(nodos_en_grafo)): # Ordenar para consistencia
        if co_nodo == 'NAN': continue

        node_data = atributos_nodos.get(co_nodo, {'TIPO': 'DEFAULT', 'EST_ESTABLE': 'DEFAULT', 'Es_Parte_De_Anillo': False})
        tipo_actual = node_data.get('TIPO', 'DEFAULT')
        estado_actual = node_data.get('EST_ESTABLE', 'DEFAULT')
        es_camino_anillo = node_data.get('Es_Parte_De_Anillo', False) # Si este nodo es parte de un camino de anillo

        forma_nodo = tipo_a_forma.get(tipo_actual, tipo_a_forma['DEFAULT'])
        color_relleno_nodo = default_node_color

        if tipo_actual == 'I' and co_nodo == circuito_co_origen:
            color_relleno_nodo = interruptor_principal_color
        elif es_camino_anillo: # Prioridad si es parte de un camino de anillo
            color_relleno_nodo = camino_anillo_color
        elif estado_actual == 'CLOSED':
            color_relleno_nodo = estado_closed_color
        elif estado_actual == 'OPEN':
            color_relleno_nodo = estado_open_color
        
        dot.node(name=co_nodo, label=co_nodo, shape=forma_nodo, fillcolor=color_relleno_nodo)

    # --- Aristas (Conexiones) ---
    aristas_dibujadas = set() # Para evitar duplicar aristas
    for co_hijo in sorted(list(nodos_en_grafo)): # Iterar sobre todos los nodos que podrían ser hijos
        if co_hijo == 'NAN': continue
        
        node_data = atributos_nodos.get(co_hijo)
        if not node_data: continue

        co_padre = node_data.get('Equipo_Padre')
        # Arista Principal (aguas arriba)
        if co_padre and co_padre != 'NAN' and (co_padre, co_hijo) not in aristas_dibujadas:
            if co_padre in nodos_en_grafo: # Asegurar que el padre está definido como nodo
                 dot.edge(co_padre, co_hijo)
                 aristas_dibujadas.add((co_padre, co_hijo))

        # Arista de Cierre de Anillo
        anillo_tipo = node_data.get('Anillo_Tipo')
        if anillo_tipo and anillo_tipo != 'NAN':
            co_elemento_cierre = node_data.get('Anillo_Con_Elemento_CO')
            co_circuito_cierre = node_data.get('Anillo_Con_Circuito_CO')

            if co_elemento_cierre and co_elemento_cierre != 'NAN':
                edge_color_anillo = anillo_interno_color
                style_anillo = "dashed"
                
                # Si el anillo es con OTRO circuito, crear un nodo "cloud" para ese circuito
                if anillo_tipo == 'EXTERNO' and co_circuito_cierre != circuito_co_origen:
                    nodo_otro_circuito = f"Otro_Cto:\n{co_circuito_cierre}"
                    if nodo_otro_circuito not in nodos_en_grafo: # Añadir solo si no existe
                        dot.node(name=nodo_otro_circuito, label=nodo_otro_circuito, shape='tab', fillcolor='lightgoldenrodyellow', style='filled,dashed')
                        nodos_en_grafo.add(nodo_otro_circuito) # Marcar como añadido
                    
                    # La arista va del elemento OPEN (co_hijo, en este caso) al nodo del otro circuito.
                    # El 'Anillo_Con_Elemento_CO' es el elemento *en* el otro circuito.
                    # Para simplificar, la flecha irá del OPEN al cloud del otro circuito.
                    if (co_hijo, nodo_otro_circuito) not in aristas_dibujadas:
                        dot.edge(co_hijo, nodo_otro_circuito, style=style_anillo, color=anillo_externo_color, dir="forward", constraint="false", arrowhead="normal")
                        aristas_dibujadas.add((co_hijo, nodo_otro_circuito))
                
                # Si el anillo es INTERNO o con un elemento del mismo circuito (aunque la lógica lo clasifique como EXTERNO)
                elif co_elemento_cierre in nodos_en_grafo: # Asegurar que el elemento de cierre está en el grafo
                     # La arista va del elemento OPEN (co_hijo) al elemento_cierre
                    if (co_hijo, co_elemento_cierre) not in aristas_dibujadas:
                        dot.edge(co_hijo, co_elemento_cierre, style=style_anillo, color=edge_color_anillo, dir="forward", constraint="false", arrowhead="normal", label="Anillo")
                        aristas_dibujadas.add((co_hijo, co_elemento_cierre))
    
    os.makedirs(output_folder, exist_ok=True)
    safe_circuito_co = "".join(c if c.isalnum() or c in ('_','-') else '_' for c in circuito_co_origen)
    output_filename_base = f'circuito_con_anillos_{safe_circuito_co}'
    
    try:
        filepath = dot.render(filename=output_filename_base, directory=output_folder, format='svg', cleanup=True)
        print(f"✅ Grafo (con anillos) para {circuito_co_origen} guardado en: {filepath}")
    except graphviz.backend.execute.ExecutableNotFound:
        print("❌ ERROR CRÍTICO: Graphviz no encontrado.")
    except Exception as e:
        print(f"❌ Error al generar grafo para {circuito_co_origen}: {e}")