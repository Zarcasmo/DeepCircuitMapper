# -*- coding: utf-8 -*-
"""
Created on Tue May 20 14:27:28 2025

@author: alopagui
"""
import pandas as pd

 
# Leer los archivos de Excel
elem_corte = pd.read_excel("Data/elementos_corte.xlsx")
lineas = pd.read_excel("Data/lineas.xlsx")
circuitos = pd.read_excel("Data/circuitos.xlsx")
tranfos= pd.read_excel("Data/transformadores.xlsx")
 
# Inicializar listas para nodos y líneas
nodos = []
linea_1 = []
linea_2 = []
elementos_corte = []
tranformadores=[]
 
# Definir el circuito que se va a buscar
def evaluar_elemento_corte(nodo):
    # Filtrar por NODO1_ID
    nod_cor_ec = elem_corte[elem_corte['NODO1_ID'] == nodo]  
    if not nod_cor_ec.empty:
        nod2ec = nod_cor_ec.iloc[0]['NODO2_ID']
        if nod2ec not in nodos:
            nodos.append(nod2ec)
    else:
        # Si no se encontró por NODO1_ID, filtrar por NODO2_ID
        nod_cor_ec_ = elem_corte[elem_corte['NODO2_ID'] == nodo]  
        if not nod_cor_ec_.empty:
            cort = nod_cor_ec_.iloc[0]['CODIGO_OPERATIVO']
            cto = nod_cor_ec_.iloc[0]['CIRCUITO']
            if cort not in [ele[0] for ele in elementos_corte]:  # Verifica si 'cort' ya está en elementos_corte
                eleCor = (cort, cto)
                elementos_corte.append(eleCor)
            nod2ec = nod_cor_ec_.iloc[0]['NODO1_ID']
            if nod2ec not in nodos:
                nodos.append(nod2ec)
 
 
def relacion_tranformadores(nodo, padre):
    nod_trafo = tranfos[tranfos['NODO1_ID'] == nodo]  
    if not nod_trafo.empty:
        trafos = nod_trafo.iloc[0]['NODO_TRANSFORM_V']
 
 
# Iterar sobre los circuitos
for index, row in circuitos.iterrows():
    circuito = row.iloc[0]  # Asegúrate de que esta columna exista
    # Filtrar el DataFrame elem_corte para obtener el circuito de inicio
    circuito_inicio = elem_corte[elem_corte['CODIGO_OPERATIVO'] == circuito]
    # Verificar si se encontró el circuito de inicio
    if not circuito_inicio.empty:
        Nodo2 = circuito_inicio.iloc[0]['NODO2_ID']
        # Agregar el nodo inicial a la lista de nodos
        if Nodo2 is not None and Nodo2 not in nodos:
            nodos.append(Nodo2)
        # Filtrar las líneas que corresponden al circuito
        lineas_filtradas = lineas[lineas['CIRCUITO'] == circuito]
        # Iterar sobre los nodos
        for nodo in nodos:
            # Filtrar las líneas donde NODO1_ID es igual al nodo actual
            lin_val = lineas_filtradas[lineas_filtradas['NODO1_ID'] == nodo]
            # Verificar si hay líneas que coinciden
            if not lin_val.empty:
                # Iterar sobre todas las líneas filtradas
                for index, row in lin_val.iterrows():
                    nod2 = row['NODO2_ID']
                    if nod2 not in nodos:
                        nodos.append(nod2)  # Agregar el nuevo nodo
                        evaluar_elemento_corte(nod2)  # Evaluar el nuevo nodo
            else:
                # Si no se encontró por NODO1_ID, buscar por NODO2_ID
                lin_val2 = lineas_filtradas[lineas_filtradas['NODO2_ID'] == nodo]
                if not lin_val2.empty:  # Verificar si hay resultados
                    # Iterar sobre todas las líneas filtradas
                    for index, row in lin_val2.iterrows():
                        nod2_ = row['NODO1_ID']
                        fid = row['G3E_FID']
                        crto = row['CIRCUITO']
                        data=(fid,crto)
                        linea_2.append(data)  # Agregar la línea actual a linea_2
                        if nod2_ not in nodos:
                            nodos.append(nod2_)
                            evaluar_elemento_corte(nod2_)  # Evaluar el nuevo nodo
    else:
      #  print("No se encontró el circuito en elem_corte.")
        Nodo2 = None  # Asignar None si no se encuentra el circuito
 
# Convertir las listas en DataFrames
df_linea_2 = pd.DataFrame(linea_2, columns=['fid','CIRCUITO'])
df_EC = pd.DataFrame(elementos_corte, columns=['NODO_UBICACION_V', 'CIRCUITO'])
 
# Crear un objeto ExcelWriter para guardar múltiples hojas
with pd.ExcelWriter('datos.xlsx') as writer:
    df_linea_2.to_excel(writer, sheet_name='Linea 2', index=False)
    df_EC.to_excel(writer, sheet_name='EC', index=False)
    
    
