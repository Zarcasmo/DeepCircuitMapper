# main.py
import pandas as pd
import time

# ---- Importar funciones del core del barrido y otras dependencias ----
from barrido_electrico import generar_dfs_resultados_finales, summarize_by_circuito
from Data_process import cargar_datos 
from visualizacion_grafos import generar_grafo_circuito 

# ----------------------------------------------------------
# Configuración Principal y Ejecución
# ----------------------------------------------------------
if __name__ == "__main__":
    # --- Parámetros de Configuración ---
    # Tipo de carga de datos: "CSV" o "oracle"
    DATA_LOAD_METHOD = "oracle" 
    Guardar_resultados = True

    # Rutas a los archivos de datos (ajustar según DATA_LOAD_METHOD)
    if DATA_LOAD_METHOD == "CSV":
        # Rutas para carga desde CSV
        path_archivo_circuitos = "Data/CSV/circuitos1.csv" # Corregido a 'circuitos.csv' como en tu último bloque
        path_archivo_elementos_corte = "Data/CSV/elementos_corte.csv"
        path_archivo_lineas = "Data/CSV/Lineas.csv"
        path_archivo_trafos = "Data/CSV/transformadores.csv"
        data_source_types_list = ["csv", "csv", "csv", "csv"]
    else:  # Asumiendo "oracle"
        # Rutas para carga desde SQL/Oracle (o mixto)
        path_archivo_circuitos = "Data/CSV/circuitos1.csv" # Sigue siendo CSV
        path_archivo_elementos_corte = "Data/SQL/elementos_corte.sql"
        path_archivo_lineas = "Data/SQL/Lineas.sql"
        path_archivo_trafos = "Data/SQL/transformadores.sql"
        data_source_types_list = ["csv", "oracle", "oracle", "oracle"]

    # Parámetros para la generación de grafos
    output_folder_grafos = "grafos_circuitos_ecs"
    # Puedes definir aquí más parámetros de personalización para generar_grafo_circuito si es necesario
    # ej: grafo_font_size = 8, grafo_default_color = 'lightskyblue', etc.

    # Parámetro de verbosidad para los barridos (controla prints detallados)
    verbose_mode = False # Cambiar a True para más detalles durante el análisis de anillos

    # --- Inicio del Proceso ---
    start_total_time = time.time()
    print("🔌 Proceso de barrido iterativo de conectividad eléctrica - EDEQ ")

    # 1. Carga de Datos
    df_circuitos_data, df_ecs_data, df_lins_data, df_trafos_data = cargar_datos(
        file_circuitos_location=path_archivo_circuitos,
        file_elementos_corte_location=path_archivo_elementos_corte,
        file_lineas_location=path_archivo_lineas,
        file_trafos_location=path_archivo_trafos,
        source_types=data_source_types_list
    )

    if df_circuitos_data is not None and df_ecs_data is not None and \
       df_lins_data is not None and df_trafos_data is not None:

        # 2. Generación de Resultados del Barrido
        df_res_ecs, df_res_lins, df_res_trafos = generar_dfs_resultados_finales(
            df_circuitos_data, 
            df_ecs_data, 
            df_lins_data, 
            df_trafos_data, 
            verbose=verbose_mode
        )
        
        if df_res_ecs is not None and df_res_lins is not None and df_res_trafos is not None:
            print(f"🎉 ¡Barridos completados (incluyendo análisis de anillos y trafos)!")
            
            summary_df = summarize_by_circuito(df_res_ecs, df_res_lins, df_res_trafos)
            print(f" \n📊 Resultados del proceso del barrido iterativo por circuito:")
            print(summary_df)
            
            #Opcional: Guardar resultados en archivos Excel
            if Guardar_resultados:
                try:
                    output_excel_file = "Reports/resultados_barrido_electrico.xlsx"
                    with pd.ExcelWriter(output_excel_file) as writer:
                        if not df_res_ecs.empty:
                            df_res_ecs.to_excel(writer, sheet_name='Elementos_Corte', index=False)
                        if not df_res_lins.empty:
                            df_res_lins.to_excel(writer, sheet_name='Lineas', index=False)
                        if not df_res_trafos.empty:
                            df_res_trafos.to_excel(writer, sheet_name='Transformadores', index=False)
                        if not summary_df.empty:
                            summary_df.to_excel(writer, sheet_name='TD', index=False)
                    print(f"✅ Resultados guardados en '{output_excel_file}'")
                except Exception as e:
                    print(f"❌ Error al guardar resultados en Excel: {e}")


            # 3. Generación de Grafos (si hay resultados de ECs)
            if not df_res_ecs.empty:
                print("\n📊 Iniciando generación de grafos para elementos de corte por circuito...")
                
                # Asegurarse que la columna para identificar circuitos únicos exista
                if 'Circuito_Origen_Barrido' in df_res_ecs.columns:
                    circuitos_unicos_en_resultados = df_res_ecs['Circuito_Origen_Barrido'].dropna().unique()
                    
                    total_grafos_a_generar = len(circuitos_unicos_en_resultados)
                    start_time_generacion_grafos = time.time()
                    
                    for i, circuito_co_actual in enumerate(circuitos_unicos_en_resultados):
                        print(f"  Generando grafo para el circuito: {circuito_co_actual} ({i+1}/{total_grafos_a_generar})...")
                        
                        df_datos_circuito_actual = df_res_ecs[
                            df_res_ecs['Circuito_Origen_Barrido'] == circuito_co_actual
                        ].copy()

                        if not df_datos_circuito_actual.empty:
                            # Llamada a la función de visualización_grafos.py
                            generar_grafo_circuito(
                                df_datos_circuito=df_datos_circuito_actual,
                                circuito_co_origen=circuito_co_actual,
                                output_folder=output_folder_grafos
                                # Se pueden pasar aquí los parámetros de personalización definidos arriba
                                # font_size=grafo_font_size, etc.
                            )
                        else:
                            print(f"    ℹ️ No hay datos de Elementos de Corte para el circuito {circuito_co_actual} para generar grafo.")
                else:
                    print("    ⚠️ No se encontró la columna 'Circuito_Origen_Barrido' en los resultados de Elementos de Corte. No se pueden generar grafos por circuito.")
            else:
                print("\nℹ️ No hay resultados de Elementos de Corte para generar grafos.")
        else:
            print("\n⚠️ No se pudieron generar los DataFrames de resultados del barrido.")
    else:
        print("\n❌ Error en la carga de datos inicial. El proceso no puede continuar.")

    # --- Fin del Proceso ---
    end_total_time = time.time()
    total_execution_time_seconds = end_total_time - start_total_time
    total_execution_time_formatted = time.strftime("%H:%M:%S", time.gmtime(total_execution_time_seconds))
    print(f"\n🏁 Proceso completo. Tiempo total de ejecución: {total_execution_time_formatted}")