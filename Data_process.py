import pandas as pd
from sqlalchemy import create_engine

def _read_sql_from_file(sql_file_path):
    """
    Lee una consulta SQL desde un archivo de texto.
    (Esta función es la misma que en la versión anterior)
    """
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        raise
    except Exception as e:
        raise

def cargar_datos(
    file_circuitos_location,
    file_elementos_corte_location,
    file_lineas_location,
    file_trafos_location,
    source_types: list, # Ejemplo: ['csv', 'oracle', 'oracle', 'oracle']
    verbose = False
):
    """
    Carga y procesa datos, especificando la fuente (CSV u Oracle) para cada archivo individualmente.

    Valida columnas, convierte a tipos de datos especificados (str, int, float, bool),
    elimina espacios y convierte a mayúsculas para strings según sea necesario.

    Args:
        file_circuitos_location (str): Ruta al archivo CSV o .sql para circuitos.
        file_elementos_corte_location (str): Ruta al archivo CSV o .sql para elementos de corte.
        file_lineas_location (str): Ruta al archivo CSV o .sql para líneas.
        source_types (list): Lista de 3 strings indicando la fuente para cada archivo
                             en el orden: circuitos, elementos_corte, lineas.
                             Cada string puede ser 'csv' o 'oracle'.
        db_connection (cx_Oracle.Connection, optional): Conexión activa a Oracle.
                                                       Requerida si alguna fuente es 'oracle'.

    Returns:
        tuple: (df_circuitos, df_elementos_corte, df_lineas) o (None, None, None, None) si hay error.
    """

    if not isinstance(source_types, list) or len(source_types) != 4:
        print("❌ Error: El parámetro 'source_types' debe ser una lista con exactamente 4 elementos.")
        return None, None, None, None

    # Mapeo de identificadores a las ubicaciones y tipos de fuente
    locations_map = {
        "df_circuitos": file_circuitos_location,
        "df_elementos_corte": file_elementos_corte_location,
        "df_lineas": file_lineas_location,
        "df_trafos": file_trafos_location
    }
    
    source_types_map = {
        "df_circuitos": source_types[0].lower(),
        "df_elementos_corte": source_types[1].lower(),
        "df_lineas": source_types[2].lower(),
        "df_trafos": source_types[3].lower()
    }

    # Verificar si se necesita conexión a Oracle y si está disponible
    is_oracle_in_sources = any(st == 'oracle' for st in source_types_map.values())
    if is_oracle_in_sources:
        try:
            # Detalles de tu conexión
            user = 'USER'
            password = 'PASS'
            host = 'HOST'
            port = 1234
            service_name = 'SERVICE'
            # Crea la URI de conexión
            database_uri = f'oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service_name}'
            # Crea el engine usando SQLAlchemy
            db_connection = create_engine(database_uri)
        except Exception:
            print("❌ Error: Al menos un archivo se especificó con fuente 'oracle', pero no se proporcionó 'db_connection'.")
            return None, None, None, None

    # Plantillas de configuración base (igual que en cargar_datos_flexible)
    # Define aquí los tipos de datos esperados y las transformaciones.
    base_configs_templates = [
        {
            "id": "df_circuitos",
            "expected_cols": ["Circuito"],
            "cols_to_process": {
                "Circuito": {"type": "str", "strip": True, "to_upper": False}
            }
        },
        {
            "id": "df_elementos_corte",
            "expected_cols": ["G3E_FID", "NODO1_ID", "NODO2_ID", "CODIGO_OPERATIVO", "CIRCUITO", "TIPO", "EST_ESTABLE"],
            "cols_to_process": {
                "G3E_FID": {"type": "str", "strip": True},
                "NODO1_ID": {"type": "str", "strip": True},
                "NODO2_ID": {"type": "str", "strip": True},
                "CODIGO_OPERATIVO": {"type": "str", "strip": True},
                "CIRCUITO": {"type": "str", "strip": True},
                "TIPO": {"type": "str", "strip": True, "to_upper": True},
                "EST_ESTABLE": {"type": "str", "strip": True, "to_upper": True},
            }
        },
        {
            "id": "df_lineas",
            "expected_cols": ["G3E_FID", "NODO1_ID", "NODO2_ID", "CIRCUITO"],
            "cols_to_process": {
                "G3E_FID": {"type": "str", "strip": True},
                "NODO1_ID": {"type": "str", "strip": True},
                "NODO2_ID": {"type": "str", "strip": True},
                "CIRCUITO": {"type": "str", "strip": True}
            }
        },
        {
            "id": "df_trafos",
            "expected_cols": ["G3E_FID", "CODIGO", "NODO1_ID", "NODO2_ID", "CIRCUITO"],
            "cols_to_process": {
                "G3E_FID": {"type": "str", "strip": True},
                "CODIGO": {"type": "str", "strip": True},
                "NODO1_ID": {"type": "str", "strip": True},
                "NODO2_ID": {"type": "str", "strip": True},
                "CIRCUITO": {"type": "str", "strip": True}
            }
        }
    ] # Fin de base_configs_templates

    file_processing_configs = []
    for config_template in base_configs_templates:
        current_config = config_template.copy() # Crear copia para modificar
        file_id = current_config["id"]

        current_config["location"] = locations_map[file_id]
        current_config["data_source"] = source_types_map[file_id]

        if current_config["data_source"] not in ['csv', 'oracle']:
            print(f"❌ Error: Tipo de fuente '{current_config['data_source']}' para el archivo '{file_id}' no es válido. Use 'csv' o 'oracle'.")
            return None, None, None, None
        
        file_processing_configs.append(current_config)

    loaded_dataframes = {}

    # --- PASO 1: Cargar todos los DataFrames según data_source individual ---
    print(f"🔄 Iniciando carga de datos...")
    for config in file_processing_configs:
        file_id = config["id"]
        location = config["location"]
        current_data_source = config["data_source"]
        if verbose: print(f"  Intentando cargar datos para '{file_id}' desde '{current_data_source.upper()}' en: {location}")

        if current_data_source == 'csv':
            try:
                loaded_dataframes[file_id] = pd.read_csv(location, delimiter=";")
                print(f"  ✅ Archivo CSV '{location}' leido exitosamente.")
            except FileNotFoundError:
                print(f"❌ Error: Archivo CSV no encontrado - {location}")
                return None, None, None, None
            except Exception as e:
                print(f"❌ Error al leer el archivo CSV '{location}': {e}")
                return None, None, None, None
        elif current_data_source == 'oracle':
            # La comprobación de db_connection ya se hizo si 'oracle' está en las fuentes
            try:
                sql_query = _read_sql_from_file(location)
                if verbose: print(f"    Consulta SQL leída desde '{location}'. Ejecutando...")
                loaded_dataframes[file_id] = pd.read_sql_query(sql_query, db_connection)
                
                loaded_dataframes[file_id].columns = [col.upper() for col in loaded_dataframes[file_id].columns]

                print(f"  ✅ Datos para '{file_id}' leidos exitosamente desde Oracle.")
            except FileNotFoundError:
                print(f"❌ Error: Archivo SQL no encontrado - {location}")
                return None, None, None, None
            except Exception as e:
                print(f"❌ Error al cargar datos desde Oracle para '{file_id}' (SQL file: {location}): {e}")
                return None, None, None, None
    print("👍 Todos los datos fueron cargados inicialmente.")

    # --- PASO 2: Validar columnas y Procesar (convertir tipos, transformar) ---
    # Esta sección es idéntica a la de cargar_datos_flexible, solo se ajustan los mensajes
    # para incluir la 'location' de donde se cargó el archivo para mayor claridad.
    print("\n🔄 Iniciando preprocesamiento de DataFrames cargados...")
    for config in file_processing_configs:
        file_id = config["id"]
        df = loaded_dataframes[file_id]
        original_location = config["location"] # Para mensajes de error más claros
        if verbose: print(f"  Procesando DataFrame '{file_id}' (cargado desde: {original_location})")

        # 2a. Validar columnas esperadas
        missing_cols = [col for col in config["expected_cols"] if col not in df.columns]
        if missing_cols:
            print(f"❌ Error: Faltan columnas cruciales en los datos de '{file_id}' (desde {original_location}).")
            print(f"  Columnas del DataFrame: {list(df.columns)}")
            print(f"  Columnas esperadas: {config['expected_cols']}")
            print(f"  Columnas faltantes detectadas: {missing_cols}")
            return None, None, None, None
        if verbose: print(f"    ✅ Validación de columnas para '{file_id}' exitosa.")

        # 2b. Procesar columnas: convertir tipos y aplicar transformaciones
        if verbose: print(f"    🔄 Aplicando transformaciones y tipos de datos para '{file_id}'...")
        for col_name, processing_rules in config["cols_to_process"].items():
            if col_name not in df.columns:
                print(f"⚠️ Advertencia: Columna '{col_name}' para procesar no encontrada en '{file_id}'. Se omite.")
                continue

            target_type_str = processing_rules["type"]
            if verbose: print(f"      Procesando columna '{col_name}' al tipo '{target_type_str}'...")
            try:
                if target_type_str == "str":
                    df[col_name] = df[col_name].astype(str)
                    if processing_rules.get("strip", False): df[col_name] = df[col_name].str.strip()
                    if processing_rules.get("to_upper", False): df[col_name] = df[col_name].str.upper()
                
                elif target_type_str == "int":
                    numeric_col = pd.to_numeric(df[col_name], errors='raise')
                    if numeric_col.notna().any() and not numeric_col.dropna().apply(lambda x: x == int(x)).all():
                         raise ValueError(f"contiene valores flotantes (ej. {numeric_col.dropna().iloc[0]}) que no son enteros exactos.")
                    df[col_name] = numeric_col.astype('Int64')

                elif target_type_str == "float":
                    df[col_name] = pd.to_numeric(df[col_name], errors='raise').astype(float)

                elif target_type_str == "bool":
                    if pd.api.types.is_string_dtype(df[col_name]) or df[col_name].dtype == object:
                        s_col_str = df[col_name].astype(str).str.lower().str.strip()
                        bool_map = {
                            'true': True, '1': True, 'yes': True, 't': True, 'verdadero': True, 'si': True, 's': True,
                            'false': False, '0': False, 'no': False, 'f': False, 'falso': False,
                            '': pd.NA, 'nan': pd.NA, 'none': pd.NA, '<na>': pd.NA
                        }
                        df[col_name] = s_col_str.map(bool_map)
                    df[col_name] = df[col_name].astype('boolean')
                else:
                    df[col_name] = df[col_name].astype(target_type_str)
                if verbose: print(f"        ✅ Columna '{col_name}' convertida a '{df[col_name].dtype}'.")

            except Exception as e_conv:
                print(f"❌ Error de Conversión en '{file_id}' (desde {original_location}):")
                print(f"  No se pudo convertir la columna '{col_name}' al tipo '{target_type_str}'.")
                print(f"  Error detallado: {e_conv}")
                return None, None, None, None
        print(f"    ✅ Transformaciones para '{file_id}' completadas.")

    print("\n👍 ¡Todos los DataFrames han sido cargados y preprocesados exitosamente")
    return loaded_dataframes["df_circuitos"], loaded_dataframes["df_elementos_corte"], loaded_dataframes["df_lineas"], loaded_dataframes["df_trafos"]