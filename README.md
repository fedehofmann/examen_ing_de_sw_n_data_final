# Medallion Architecture Demo (Airflow + dbt + DuckDB)

Este proyecto crea un pipeline de 3 pasos que replica la arquitectura medallion:

1. **Bronze**: Airflow lee un CSV crudo según la fecha de ejecución y aplica una limpieza básica con Pandas, guardando un archivo parquet limpio.

2. **Silver**: Un `dbt run` lee el parquet limpio de `data/clean/transactions_<ds_nodash>_clean.parquet` y materializa dos modelos dentro del archivo de base de datos `warehouse/medallion.duckdb`:
   - Una tabla/vista de staging por transacción (`stg_transactions`) con tipos normalizados.
   - Una tabla de marts agregada por cliente (`fct_customer_transactions`) lista para análisis.

3. **Gold**: `dbt test` se conecta a ese mismo warehouse en DuckDB, ejecuta pruebas de calidad (built-in y custom) sobre esos modelos y escribe un archivo JSON por fecha (`data/quality/dq_results_<ds_nodash>.json`) con el resultado.

## Estructura

```text
├── dags/
│   └── medallion_medallion_dag.py
├── data/
│   ├── raw/
│   │   └── transactions_YYYYMMDD.csv
│   ├── clean/
│   │   └── transactions_YYYYMMDD_clean.parquet
│   └── quality/
│       └── dq_results_YYYYMMDD.json
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_transactions.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── fct_customer_transactions.sql
│   │       └── schema.yml
│   └── tests/
│       └── generic/
│           ├── non_negative.sql
│           ├── non_zero.sql
│           └── max_value.sql
├── include/
│   └── transformations.py
├── profiles/
│   └── profiles.yml
├── warehouse/
│   └── medallion.duckdb # Se genera en tiempo de ejecución
├── airflow_home/
├── README.md
└── requirements.txt
```

## Requisitos

- Python 3.10+
- DuckDB CLI (opcional, solo para ejecutar los comandos de verificación por terminal)

Instalar dependencias:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

DuckDB CLI (opcional, recomendado para inspeccionar la base desde la terminal):

```bash
brew install duckdb # En macOS con Homebrew
```

## Configuración de variables de entorno

Antes de iniciar Airflow necesitamos definir algunas variables de entorno para que las herramientas “sepan” dónde está cada componente del proyecto:

- `AIRFLOW_HOME`: carpeta donde Airflow guarda su metadata, logs y configuración.
- `DBT_PROFILES_DIR`: ruta al `profiles.yml` que usa dbt para conectarse a DuckDB.
- `DUCKDB_PATH`: ubicación del archivo de base de datos `medallion.duckdb`.
- `AIRFLOW__CORE__DAGS_FOLDER`: carpeta donde Airflow busca los DAGs del proyecto.
- `AIRFLOW__CORE__LOAD_EXAMPLES`: se pone en `False` para no cargar los DAGs de ejemplo.

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export DBT_PROFILES_DIR=$(pwd)/profiles
export DUCKDB_PATH=$(pwd)/warehouse/medallion.duckdb
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

IMPORTANTE - Para que el DAG se importe correctamente a Airflow en sistema operativo MacOS debemos correr el siguiente comando:

```bash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

## Inicializar Airflow

```bash
airflow standalone
```

En el output de `airflow standalone` buscar la contraseña para loguearse. Por ejemplo:

```
standalone | Airflow is ready
standalone | Login with username: admin  password: pPr9XXxFzgrgGd6U
```

## Ejecutar el DAG

1. Preparar el archivo crudo (Bronze)

Colocar/actualizar el archivo en:
```text
data/raw/transactions_YYYYMMDD.csv
```

El nombre de archivo debe coincidir con la logical date (ds_nodash) de la corrida. En la práctica, la idea es tener un archivo por día (por ejemplo, exportado del sistema transaccional) y nombrarlo con la fecha de esas transacciones. Cuando el DAG corre para una logical date (Airflow la expone como ds_nodash), busca exactamente ese archivo. De esta forma, el pipeline puede re-procesar cualquier día histórico simplemente cambiando la fecha de ejecución del DAG.

2. Triggerear el DAG desde la UI, o CLI utilizando el siguiente comando:

```bash
airflow dags trigger medallion_pipeline --run-id manual_$(date +%s)
```

El DAG ejecutará:

- `bronze_clean`: llama a `clean_daily_transactions` para crear `data/clean/transactions_<ds>_clean.parquet`.
- `silver_dbt_run`: ejecuta `dbt run` apuntando al proyecto `dbt/` y carga la data en DuckDB.
- `gold_dbt_tests`: corre `dbt test` y escribe `data/quality/dq_results_<ds>.json` con el status (`passed`/`failed`).

Si un test falla, el archivo igual se genera y el task termina en error para facilitar el monitoreo.

## Ejecutar dbt manualmente

Aunque el flujo completo está orquestado por Airflow, a veces es útil correr `dbt run` y `dbt test` directamente desde la terminal para debuggear modelos o pruebas de calidad de datos.

1. Posicionarse en la carpeta del proyecto de dbt:

```bash
cd dbt
```

2. Asegurarse de exportar variables de entorno necesarias (`CLEAN_DIR`, `DS_NODASH` y `DUCKDB_PATH`):

```bash
export DBT_PROFILES_DIR=$(pwd)/../profiles
export CLEAN_DIR=$(pwd)/../data/clean
export DS_NODASH=20251205 # Fecha que se quiera probar (YYYYMMDD)
export DUCKDB_PATH=$(pwd)/../warehouse/medallion.duckdb
```

3. Ejecutar modelos y pruebas:

```bash
dbt run
dbt test
```

## Observabilidad de Data Quality

Cada corrida crea `data/quality/dq_results_<ds>.json` con información mínima sobre el estado de los tests, por ejemplo:

```json
{
  "ds_nodash": "20251205",
  "status": "passed",
  "stdout": "...",
  "stderr": ""
}
```

Ese archivo puede ser consumido por otras herramientas para auditoría o alertas.

### dbt Tests

- Tests nativos de dbt: `not_null`, `unique`, `accepted_values`.
- Tests custom (en `dbt/tests/generic`): `non_negative` (sin negativos/NULL), `non_zero` (sin ceros/NULL) y `max_value` (límites configurables por columna en `schema.yml`).

Impacto por modelo:

- `stg_transactions` (staging):
  - `transaction_id`: `not_null`, `unique`.
  - `customer_id`: `not_null`.
  - `amount`: `not_null`, `non_negative`, `non_zero`, `max_value` (actualmente 1.000.000, configurable).
  - `status`: `not_null`, `accepted_values` (`completed`, `pending`, `failed`).
  - `transaction_ts`, `transaction_date`: `not_null`.

- `fct_customer_transactions` (marts):
  - `customer_id`: `not_null`.
  - `transaction_count`: `not_null`, `non_negative`, `non_zero`.
  - `total_amount_completed`: `not_null`, `non_negative`, `max_value` (actualmente 1.000.000, configurable).
  - `total_amount_all`: `not_null`, `non_negative`.


## Verificación de resultados por capa

### Bronze

1. Revisar que exista el parquet más reciente:

```bash
$ find data/clean/ | grep transactions_*
data/clean/transactions_20251201_clean.parquet
```

2. Inspeccionar las primeras filas para confirmar la limpieza aplicada:

```bash
duckdb -c "
SELECT 
  *
FROM read_parquet('data/clean/transactions_20251201_clean.parquet')
LIMIT 5;
"
```

### Silver

1. Abrir el warehouse y listar las tablas creadas por dbt:

```bash
duckdb warehouse/medallion.duckdb -c ".tables"
```

Deberían aparecer al menos los modelos `stg_transactions` y `fct_customer_transactions`.

2. Ejecutar consultas puntuales para validar cálculos intermedios:

```bash
duckdb warehouse/medallion.duckdb -c "
SELECT 
  *
FROM stg_transactions
LIMIT 5;
"

duckdb warehouse/medallion.duckdb -c "
SELECT 
  *
FROM fct_customer_transactions
LIMIT 10;
"
```

`stg_transactions` y `fct_customer_transactions` viven dentro de `warehouse/medallion.duckdb`.

### Gold

1. Revisar que exista el archivo de data quality más reciente:

```bash
$ find data/quality/*.json
data/quality/dq_results_20251201.json
```

2. Confirmar la generación del archivo de data quality:

```bash
cat data/quality/dq_results_20251201.json | jq
```

3. En caso de fallos, inspeccionar `stderr` dentro del mismo JSON o revisar los logs del task en la UI/CLI de Airflow para identificar la prueba que reportó error.