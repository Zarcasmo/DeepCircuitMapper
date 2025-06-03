# Iterative Electrical Sweep - DFS

## Overview

This repository contains a Python implementation of an **iterative electrical sweep** for analyzing electrical distribution networks. The system processes data on circuits, switching elements (ECs), lines, and transformers to determine connectivity, identify energized components, and detect loops (anillos) or transfers between circuits. The results are visualized as directed graphs in SVG format, representing the topology of each circuit.

The codebase is designed to be flexible, supporting data loading from CSV files or Oracle databases, and includes robust functionalities for data validation, processing, and visualization. The primary use case is the analysis of electrical distribution networks for the Empresa de Energía del Quindío (EDEQ).

## Repository Structure

The repository is organized into the following main files:

- **`main.py`**: The main script that orchestrates the entire process, from data loading to executing the electrical sweep and generating graphs.
- **`Data_process.py`**: Manages data loading from CSV or Oracle sources, with validation and preprocessing of dataframes.
- **`barrido_electrico.py`**: Contains the core logic for the iterative electrical sweep and loop analysis.
- **`visualizacion_grafos.py`**: Generates directed graphs in SVG format to visualize circuit topologies.
- **Data Files**:
  - `Data/CSV/`: Directory for input CSV files (`circuitos.csv`, `elementos_corte.csv`, `Lineas.csv`, `transformadores.csv`).
  - `Data/SQL/`: Directory for SQL query files (`elementos_corte.sql`, `Lineas.sql`, `transformadores.sql`) for Oracle database access.

## Functionality

The system performs the following high-level tasks:

1. **Data Loading and Preprocessing** (`Data_process.py`):
   - Loads data for circuits, switching elements (ECs), lines, and transformers from CSV files or an Oracle database.
   - Validates required columns and applies data type conversions (e.g., string stripping, uppercase conversion).
   - Ensures data consistency by handling missing values and enforcing expected formats.

2. **Iterative Electrical Sweep** (`barrido_electrico.py`):
   - Executes a depth-first search (DFS) to trace electrical connectivity from a circuit’s main switch.
   - Identifies energized components (ECs, lines, transformers) based on the state of switching elements (OPEN or CLOSED).
   - Performs a secondary sweep to detect loops or transfers by analyzing OPEN switching elements.
   - Accumulates results in dataframes with hierarchical relationships (e.g., parent equipment, upstream elements).

3. **Graph Visualization** (`visualizacion_grafos.py`):
   - Generates directed graphs for each circuit using Graphviz, saved as SVG files.
   - Represents switching elements as nodes with shapes and colors based on their type and state.
   - Displays connections (edges) between elements, highlighting loops with dashed lines.

4. **Main Execution** (`main.py`):
   - Coordinates the entire workflow, from data loading to result generation and visualization.
   - Allows configuration of data sources (CSV or Oracle) and optional saving of results to an Excel file.
   - Tracks execution time and provides progress feedback.

## Logic of the Iterative Electrical Sweep

The iterative electrical sweep is designed to model the connectivity and energization state of an electrical distribution network. The process consists of two main phases: **connectivity sweep** and **loop analysis**.

### Phase 1: Connectivity Sweep

The connectivity sweep uses a depth-first search (DFS) algorithm to trace the electrical path from a circuit’s main switch (identified by `CODIGO_OPERATIVO` in the circuit data). The logic is implemented in the `barrido_conectividad_por_circuito` function in `barrido_electrico.py`. Here’s how it works:

1. **Initialization**:
   - The sweep begins at the circuit’s main switch, identified by matching the `CODIGO_OPERATIVO` in the switching elements dataframe (`df_elementos_corte_global`).
   - A stack (`pila_exploracion`) is used to manage the DFS, starting with the second node (`NODO2_ID`) of the main switch.

2. **Exploration**:
   - For each element in the stack (a switching element or a line), the algorithm:
     - Identifies connected lines via `NODO1_ID` or `NODO2_ID`.
     - Adds lines to the results (`resultados_lineas_global_lista`) with their parent switching element and upstream path.
     - Searches for transformers connected to the line’s nodes and adds them to the results (`resultados_transformadores_global_lista`).
     - Identifies connected switching elements (ECs) and processes them based on their state:
       - **CLOSED**: Continues the sweep through the opposite node (`NODO1_ID` or `NODO2_ID`), adding the EC to the results and updating the upstream path.
       - **OPEN**: Marks the EC as a potential loop point, recording the unexplored node (`Nodo_No_Explorado_Anillo`) for later analysis.
       - **Interconnection ECs**: If an EC belongs to a different circuit, it is treated as OPEN for loop analysis, preserving its original state.

3. **Tracking**:
   - Visited elements are tracked using sets (`visitados_ec_fids_este_circuito`, `visitados_lineas_fids_este_circuito`, `visitados_trafos_fids_este_circuito`) to avoid cycles within the circuit.
   - Results include hierarchical information (`Equipo_Padre`, `Elementos_Aguas_Arriba`, `Circuito_Origen_Barrido`).

4. **Progress Feedback**:
   - A progress bar (`print_progress_bar`) displays the status of the sweep across all circuits, including elapsed time.

### Phase 2: Loop Analysis

The loop analysis, implemented in `barrido_anillos_especifico` and orchestrated in `generar_dfs_resultados_finales`, identifies loops or transfers by exploring the unexplored nodes of OPEN switching elements. The logic is as follows:

1. **Identification of OPEN ECs**:
   - For each circuit, the algorithm filters OPEN switching elements with a valid `Nodo_No_Explorado_Anillo` from the connectivity sweep results.
   - These ECs are potential points where the circuit connects to another circuit (forming a loop or transfer).

2. **Exploration**:
   - A simplified DFS starts from the unexplored node of each OPEN EC.
   - The sweep searches for the first switching element encountered, which may belong to the same circuit (internal loop) or a different circuit (transfer).
   - Lines and CLOSED ECs are traversed, but only the first EC encountered is recorded as the loop’s endpoint.

3. **Results**:
   - For each OPEN EC, the algorithm records:
     - `Equipo_anillo`: The `CODIGO_OPERATIVO` of the EC that closes the loop.
     - `Elementos_Aguas_Arriba_anillo`: The upstream path to the loop’s endpoint.
     - `Circuito_anillo`: The circuit of the endpoint EC (same or different from the starting circuit).
   - If no EC is found within a maximum iteration limit (100), the loop analysis returns `pd.NA` for all fields.

4. **Progress Feedback**:
   - A separate progress bar tracks the loop analysis for each circuit.

### Outputs

- **Dataframes**:
  - `df_final_elementos_corte`: Contains switching elements with connectivity and loop information.
  - `df_final_lineas`: Contains energized lines with their parent ECs and upstream paths.
  - `df_final_trafos`: Contains transformers connected to energized lines.

- **Visualizations**:
   - Directed graphs are generated for each circuit using Graphviz, saved as SVG files in the `grafos_circuitos_ecs` directory.
   - Nodes represent switching elements with shapes based on their type (e.g., diamond for switches, ellipse for circuit breakers) and colors based on their state (e.g., green for CLOSED, red for OPEN).
   - Edges represent physical connections, with dashed lines for loops (blue for internal, orange for external).

## Prerequisites

To run the code, ensure the following dependencies are installed:

```bash
pip install pandas graphviz sqlalchemy cx_Oracle colorama
```

Additionally, Graphviz must be installed on the system and added to the PATH:

- **Windows**: Download and install Graphviz from [https://graphviz.org/download/](https://graphviz.org/download/) and add the `bin` directory to the PATH.
- **Linux/Mac**: Install Graphviz using the package manager (e.g., `sudo apt-get install graphviz` on Ubuntu).

For Oracle database access, ensure the Oracle client libraries are installed and configured.

## Usage

1. **Configure Data Sources**:
   - Edit `main.py` to set `DATA_LOAD_METHOD` to `"CSV"` or `"oracle"`.
   - Update the paths for CSV or SQL files in the `Data/CSV/` or `Data/SQL/` directories.
   - For Oracle, verify the connection details (user, password, host, port, service name) in `Data_process.py`.

2. **Run the Script**:
   ```bash
   python main.py
   ```

3. **Outputs**:
   - **Dataframes**: Results are stored in memory and can be saved to an Excel file (`resultados_barrido_electrico_final.xlsx`) if `Guardar_resultados` is set to `True`.
   - **Graphs**: SVG files are generated in the `grafos_circuitos_ecs` directory, one per circuit.

## Example Results
## Example Results

### Example of the Final Switching Elements Dataframe
| G3E_FID  | CIRCUIT | OPERATIONAL_CODE | STABLE_STATE | PARENT_EQUIPMENT | UPSTREAM_ELEMENTS                  | LOOP_EQUIPMENT | UPSTREAM_LOOP_ELEMENTS               | LOOP_CIRCUIT |
|----------|---------|------------------|--------------|------------------|-----------------------------------|----------------|-------------------------------------|--------------|
| 30748671 | 111-24- | S-1871           | CLOSED       | C-0658           | 111-24-,C-0180,C-0276,C-0658      |                |                                     |              |
| 47047337 | 111-24- | C-0983           | OPEN         | C-0658           | 111-24-,C-0180,C-0276,C-0658      | C-0984         | 111-24-,C-0180,C-0276,C-0658,C-0982,R-235 | 111-24- |
| 47047332 | 111-24- | C-0982           | CLOSED       | C-0658           | 111-24-,C-0180,C-0276,C-0658      |                |                                     |              |
| 47047319 | 111-24- | R-235            | CLOSED       | C-0982           | 111-24-,C-0180,C-0276,C-0658,C-0982 |                |                                     |              |
| 47047327 | 111-24- | C-0984           | CLOSED       | R-235            | 111-24-,C-0180,C-0276,C-0658,C-0982,R-235 |                |                                     |              |

### Example of the Final Electrical Lines Dataframe
| G3E_FID  | CIRCUIT | VOLTAGE | PARENT_EQUIPMENT | UPSTREAM_ELEMENTS                  |
|----------|---------|---------|------------------|-----------------------------------|
| 48263495 | 111-24- | 13.2    | C-0180           | 111-24-,C-0180                    |
| 48263614 | 111-24- | 13.2    | C-0180           | 111-24-,C-0180                    |
| 30745695 | 111-24- | 13.2    | C-0276           | 111-24-,C-0180,C-0276             |
| 47201602 | 111-24- | 13.2    | S-2463           | 111-24-,C-0180,C-0276,S-2463      |
| 47199053 | 111-24- | 13.2    | S-2463           | 111-24-,C-0180,C-0276,S-2463      |

### Example of the Final Transformers Dataframe
| G3E_FID  | CODE    | CIRCUIT | VOLTAGE | CONNECTION_LINE_FID | UPSTREAM_ELEMENTS                  | PARENT_LINE_EQUIPMENT |
|----------|---------|---------|---------|---------------------|-----------------------------------|-----------------------|
| 47199184 | ARUN2729 | 111-24- | 13.2    | 47199200            | 111-24-,C-0180,C-0276,S-2463      | S-2463                |
| 46671672 | ARRN0923 | 111-24- | 13.2    | 46671678            | 111-24-,C-0180,C-0276             | C-0276                |
| 30748914 | ARRN0880 | 111-24- | 13.2    | 30746039            | 111-24-,C-0180,C-0276             | C-0276                |
| 30748976 | ARUN2597 | 111-24- | 13.2    | 30746043            | 111-24-,C-0180,C-0276,S-1902,S-2281 | S-2281                |
| 30748959 | ARRN0876 | 111-24- | 13.2    | 30746000            | 111-24-,C-0180,C-0276,S-1902      | S-1902                |

## Example Visualizations
![101-24](References/101_24.png)

The diagram visualizes the `101-24` circuit and its multiple downstream connections. Based on the shape, the type of switching element (EC) is identified: knife switch, sectionalizer, or recloser. It also shows the internal loops formed, for example, between `C-0589` and `C-0591`, and external loops with other circuits, such as the `101-25` circuit, which are established through the knife switch `C-0776` or the recloser `R-171`.

## Customization

- **Graph Appearance**: Modify parameters in `generar_grafo_circuito` (e.g., `font_size`, `node_width`, `line_thickness`, colors) to customize the SVG output.
- **Data Sources**: Adjust `source_types` in `main.py` to mix CSV and Oracle inputs.
- **Verbosity**: Set `verbose_mode=True` in `main.py` for detailed logging during the sweep and loop analysis.

## Notes

- The code assumes that the input data is well-formed and contains the required columns. Missing or inconsistent data will trigger error messages and halt execution.
- The loop analysis is limited to 100 iterations per OPEN EC to prevent infinite loops in complex topologies.
- Colorama is optional for coloring the progress bar; the script includes a fallback if it is not installed.

## License

This project is for internal use by EDEQ and is not licensed for public distribution.

## Contact

For questions or support, contact the repository maintainer at [Zarcasmo Profile](https://github.com/Zarcasmo).