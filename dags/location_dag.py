from airflow import DAG
from airflow.operators.python import PythonOperator, BashOperator
from airflow.providers.amazon.aws.transfers.s3_to_s3 import S3ToS3Operator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import subprocess

# Define your default_args and DAG parameters
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'walmart_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch Walmart data, transform it, and load it into S3 with email notification',
    schedule_interval=None,  # You can define the schedule interval as needed
    catchup=False,
)


# Define a PythonOperator to execute "get_walmart_locations.py"
def get_walmart_locations():
    subprocess.run(['python', 'get_walmart_locations.py'])


fetch_walmart_data = PythonOperator(
    task_id='fetch_walmart_data',
    python_callable=get_walmart_locations,
    dag=dag,
)

# Define a PythonOperator to execute "sanitize_phone_numbers.py"
cmds = ['python sanitize_phone_numbers.py']

transform_walmart_data = BashOperator(
    task_id='transform_walmart_data',
    cmds = cmds,
    dag=dag,
)

# Define an S3ToS3Operator to upload data to S3
upload_to_s3 = S3ToS3Operator(
    task_id='upload_to_s3',
    source_bucket_name='source_bucket_name',
    source_bucket_key='path/to/transformed_data.csv',
    dest_bucket_name='destination_bucket_name',
    dest_bucket_key='path/to/uploaded_data.csv',
    aws_conn_id='aws_default',  # Configure your AWS connection ID
    replace=True,  # Set to True to overwrite existing data
    dag=dag,
)

# Define an EmailOperator to send an email upon completion
email_notification = EmailOperator(
    task_id='email_notification',
    to='recipient@example.com',
    subject='Walmart Data Pipeline Completed',
    html_content='The Walmart data pipeline has completed successfully.',
    dag=dag,
)

# Define task dependencies
fetch_walmart_data >> transform_walmart_data >> upload_to_s3 >> email_notification
