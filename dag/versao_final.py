import pandas as pd
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import io
import logging

# Configuração geral
BUCKET_GCP = 'projeto_cs_buff'
CSV_COMPRAS = 'U1106683827_20230103_20240602_buy.csv'
BIGQUERY_TABLE = 'proud-outpost-455911-s8.projeto_cs_buff.compras_final'

def read_csv(**context):
    gcs_hook = GCSHook()
    file_content = gcs_hook.download(
        bucket_name=BUCKET_GCP,
        object_name=CSV_COMPRAS
    )
    df = pd.read_csv(io.BytesIO(file_content))
    return df.to_json()

def transform_data(**context):
    csv_data = context['task_instance'].xcom_pull(task_ids='read_csv')
    df = pd.read_json(io.StringIO(csv_data))
    df['Time(GMT+8)'] = pd.to_datetime(df['Time(GMT+8)'])
    df = df.sort_values(by='Time(GMT+8)', ascending=True)

    df['id_transaction'] = df.index + 1

    df['Items'] = df['Items'].astype(str)

    wear = ['Battle-Scarred', 'Factory New', 'Minimal Wear', 'Field-Tested', 'Well-Worn']
    join_wear = f"({'|'.join(wear)})"
    df['wear'] = df['Items'].str.extract(join_wear, expand=False)

    df['store_link'] = df['Items'].str.extract(r'HYPERLINK\("([^"]+)"')

    df['item'] = df['Items'].str.extract(r',\s*"([^"]+)"\)')

    df = df.drop(columns=['Items'])

    df['Price'] = df['Price'].astype(str)
    df['Price_CNY'] = df['Price'].str.replace('¥ ', '', regex=True)
    df['Price_CNY'] = df['Price_CNY'].str.strip()
    df['Price_CNY'] = df['Price_CNY'].replace('', '0')
    df['Price_CNY'] = pd.to_numeric(df['Price_CNY'], errors='coerce').fillna(0)
    df = df.drop('Price', axis=1)

    df = df.rename(columns={'Time(GMT+8)': 'Time'})
    df['Timestamp_GMT8'] = pd.to_datetime(df['Time'])
    df['Transaction_Date'] = df['Time'].dt.date
    df = df.drop('Time', axis=1)
    
    return df.to_json()

def get_usd_rate(date_str):
    try:
        url = f"https://api.frankfurter.app/{date_str}?from=CNY&to=USD"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'rates' in data and 'USD' in data['rates']:
                return data['rates']['USD']
            else:
                logging.warning(f"Resposta inesperada da API para {date_str}: {data}")
        else:
            logging.warning(f"Erro na API para {date_str}: Status {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        logging.warning(f"Erro de request na API para {date_str}: {e}")
    except Exception as e:
        logging.warning(f"Erro inesperado ao chamar a API para {date_str}: {e}")
        
    logging.info(f"Usando taxa de fallback para a data: {date_str}")
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')

        if date_obj.year == 2024:
            if date_obj.month <= 6:
                return 0.138
            else:
                return 0.140
        else:
            return 0.14
            
    except Exception as e:
        logging.warning(f"Erro no fallback para {date_str}: {e}")
        return 0.14

def cny_to_usd(**context):
    transformed_data = context['task_instance'].xcom_pull(task_ids='transform_data')
    df = pd.read_json(io.StringIO(transformed_data))

    df['Rate'] = df['Transaction_Date'].astype(str).apply(get_usd_rate)
    df['Rate'] = df['Rate'].fillna(0.14)

    df['Price_USD'] = df['Price_CNY'] * df['Rate']
    
    return df.to_json()

def load_to_bigquery(**context):
    final_data = context['task_instance'].xcom_pull(task_ids='cny_to_usd')
    df = pd.read_json(io.StringIO(final_data))
    
    bq_hook = BigQueryHook()

    df['Transaction_Date'] = pd.to_datetime(df['Transaction_Date']).dt.strftime('%Y-%m-%d')

    df['Timestamp_GMT8'] = pd.to_datetime(df['Timestamp_GMT8']).dt.strftime('%Y-%m-%d %H:%M:%S')

    df = df.fillna('')

    numeric_columns = ['id_transaction', 'Price_CNY', 'Rate', 'Price_USD']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    string_columns = ['Game', 'Status', 'wear', 'store_link', 'item']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '')

    records = df.to_dict('records')

    clean_records = []
    for record in records:
        clean_record = {}
        for key, value in record.items():
            if pd.isna(value) or value is None or str(value) == 'nan':
                clean_record[key] = None
            elif key == 'Transaction_Date' and value:
                clean_record[key] = str(value)
            elif key == 'Timestamp_GMT8' and value:
                clean_record[key] = str(value)
            else:
                clean_record[key] = value
        clean_records.append(clean_record)
    
    schema = [
        bigquery.SchemaField("Game", "STRING"),
        bigquery.SchemaField("Status", "STRING"),
        bigquery.SchemaField("id_transaction", "INTEGER"),
        bigquery.SchemaField("wear", "STRING"),
        bigquery.SchemaField("store_link", "STRING"),
        bigquery.SchemaField("item", "STRING"),
        bigquery.SchemaField("Price_CNY", "FLOAT"),
        bigquery.SchemaField("Timestamp_GMT8", "TIMESTAMP"),
        bigquery.SchemaField("Transaction_Date", "DATE"),
        bigquery.SchemaField("Rate", "FLOAT"),
        bigquery.SchemaField("Price_USD", "FLOAT")
    ]

    client = bq_hook.get_client()

    project_id, dataset_id, table_id = BIGQUERY_TABLE.split('.')
    table_ref = client.dataset(dataset_id, project=project_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        max_bad_records=10
    )

    job = client.load_table_from_json(clean_records, table_ref, job_config=job_config)
    job.result()

def build_dag():
    default_args = {
        'owner': 'data_team',
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
    
    with DAG(
        dag_id='csgo_compras_buff_vfinal',
        default_args=default_args,
        description='Processa compras do CS2 feitas no Buff e carrega no BigQuery',
        schedule_interval='@once',
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=['csgo', 'buff', 'bigquery']
    ) as dag:

        t1 = PythonOperator(
            task_id='read_csv',
            python_callable=read_csv,
            provide_context=True
        )

        t2 = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data,
            provide_context=True
        )

        t3 = PythonOperator(
            task_id='cny_to_usd',
            python_callable=cny_to_usd,
            provide_context=True
        )

        t4 = PythonOperator(
            task_id='load_to_bigquery',
            python_callable=load_to_bigquery,
            provide_context=True
        )
        t1 >> t2 >> t3 >> t4

    return dag

globals()['csgo_compras_buff_vfinal'] = build_dag()