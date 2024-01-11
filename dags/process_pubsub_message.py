from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from google.cloud import bigquery, storage
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import pandas as pd


def retrieve_pubsub_messages_to_xcom(
        ti,
        project_id: str,
        subscription_id: str,
        max_messages: int
):
    """
    This method pulls message from Pub/Sub topic and redirect it to
    `ti` message manager in airflow.

    Args:
        project_id ->
        subscription_id ->
        max_messages ->

    """

    # Path to your service account key file
    service_account_path = 'dags/credentials.json'

    # Explicitly use service account credentials by specifying the private key file
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path
    )

    # Create a subscriber client with the credentials
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": max_messages},
        timeout=100
    )

    messages = []
    ack_ids = []
    for received_message in response.received_messages:
        messages.append(eval(received_message.message.data.decode("utf-8")))
        ack_ids.append(received_message.ack_id)

    if ack_ids:
        subscriber.acknowledge(request={"subscription": subscription_path,
                                        "ack_ids": ack_ids})

    # Push messages to XCom (internal airflow message manager) for the next task to consume
    ti.xcom_push(key='messages', value=messages)


def send_bronze_dataframe_to_bucket(ti, bucket_name, blob_name):
    '''
    This method sends provided dataframe to BigQuery table.

    Args:
        dataset_name -> name of dataset inside BigQuery
        table_name -> name of table inside dataset
    '''

    # Retrieve messages from the previous task using XCom
    messages = ti.xcom_pull(task_ids='retrieve_messages', key='messages')

    if not messages:
        print("No messages to process")
        return

    # Save message as bronze data frame and convert it to JSON
    df_bronze = pd.DataFrame(messages)
    df_bronze_as_json = df_bronze.to_json()

    # Path to your service account key file
    service_account_path = 'dags/credentials.json'

    # Create storage client to upload data to bucket
    storage_client = storage.Client.from_service_account_json(service_account_path)

    # Connect to bucket, get blob name
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(f"{blob_name}_{date.today()}")

    # Upload CSV file to blobn
    blob.upload_from_string(df_bronze_as_json, content_type='application/json')

    return df_bronze_as_json


def process_messages_with_pandas(ti):

    '''
    This method process dataframe retrieved from TI.
    Clean data, convert datatypes, changes schema.
    '''

    # Retrieve messages from the previous task using XCom
    messages = ti.xcom_pull(task_ids='retrieve_messages', key='messages')

    if not messages:
        print("No messages to process")
        return

    # Flatten nested structures and store each entry in a list
    entries = [
        {
            'city': city,
            'lon': data['lon'],
            'lat': data['lat'],
            'aqi': item['aqi'],
            'co': item['air_components']['co'],
            'no': item['air_components']['no'],
            'no2': item['air_components']['no2'],
            'o3': item['air_components']['o3'],
            'so2': item['air_components']['so2'],
            'pm2_5': item['air_components']['pm2_5'],
            'pm10': item['air_components']['pm10'],
            'nh3': item['air_components']['nh3'],
            'timestamp': item['datetime']
        }
        for city, data in messages[0].items()
        for key, item in data['history_air_pollution'].items()
    ]

    # Convert the list of entries into a dictionary with sequential keys
    entries_indexed = {index: entry for index, entry in enumerate(entries)}

    # Create DataFrame from dictionary
    df = pd.DataFrame.from_dict(entries_indexed, orient='index')

    # replace "ń" with "n" in city names
    df['city'] = df['city'].str.replace("ń", "n")

    # convert timestamp column data type to timestamp
    df['timestamp'] = df['timestamp'].astype('datetime64[s]')

    df_transformed = df.melt(id_vars=['city', 'lon', 'lat', 'timestamp'],
                             value_vars=['aqi', 'co', 'no', 'no2', 'o3',
                                         'so2', 'pm2_5', 'pm10', 'nh3'],
                             var_name=['tag_name'])
    df_transformed = df_transformed.sort_values(by=['timestamp', 'tag_name'])

    df_transformed = df_transformed[['city', 'lat', 'lon', 'tag_name',
                                     'value', 'timestamp']]

    return df_transformed


def save_processed_df_to_bigquery(dataset_name, table_name, **kwargs):

    '''
    This method sends processed data to Google Cloud BigQuery.

    Args:
    dataset_name -> name of dataset in GC BigQuery
    table_name -> name of table in dataset

    **kwargs -> required to retrieve processed dataframe from XCOM
    '''

    # Pull dataframe transformed in upstreamed task 
    df_transformed = kwargs['ti'].xcom_pull(task_ids='process_messages')

    # Path to your service account key file
    service_account_path = 'dags/credentials.json'

    # Explicitly use service account credentials by specifying the private key file
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path
    )

    bigquery_client = bigquery.Client(credentials=credentials,
                                      project=credentials._project_id)

    # Define the destination table
    table_ref = bigquery_client.dataset(dataset_name).table(table_name)

    try:
        # Create the job configuration
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        # Insert the data frame into the BigQuery table
        job = bigquery_client.load_table_from_dataframe(df_transformed,
                                                        table_ref,
                                                        job_config=job_config)
        job.result()

    except Exception as e:
        print(f"Exception occured: {e}.")
        return None

    return df_transformed


''' AIRFLOW DAG IMPLEMENTATION '''

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pubsub_pandas_storage_dag',
    default_args=default_args,
    description='A DAG to retrieve messages from Pub/Sub and process with Pandas',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

retrieve_messages = PythonOperator(
    task_id='retrieve_messages',
    python_callable=retrieve_pubsub_messages_to_xcom,
    op_kwargs={
        'project_id': 'phonic-vortex-398110',
        'subscription_id': 'pull-get-openweather-data-subscrption',
        'max_messages': 5
    },
    provide_context=True,
    dag=dag,
)

save_bronze_df = PythonOperator(
    task_id='save_bronze_df_and_send_to_bucket',
    python_callable=send_bronze_dataframe_to_bucket,
    op_kwargs={
        'bucket_name': 'air_pollution_bucket_amatacz',
        'blob_name': 'air_pollution_all_data_df_bronze.json'
    },
    provide_context=True,
    dag=dag
)

process_messages = PythonOperator(
    task_id='process_messages',
    python_callable=process_messages_with_pandas,
    provide_context=True,
    dag=dag,
)

save_to_bigquery = PythonOperator(
    task_id='save_processed_df_to_bigquery',
    python_callable=save_processed_df_to_bigquery,
    op_kwargs={
        # 'df_transformed': process_messages_with_pandas,
        'dataset_name': 'air_pollution_dataset_unified',
        'table_name': 'unified_city_data'
    },
    provide_context=True,
    dag=dag
)


retrieve_messages >> save_bronze_df >> process_messages >> save_to_bigquery
