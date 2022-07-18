from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import S3KeySensor
import boto3
import csv,io
from simple_salesforce import Salesforce
from airflow.exceptions import AirflowFailException
from datetime import datetime
now = datetime.now()
    

s3_file_buckname = 'demo1-s3-sensor'
account_file_fey = 'demo1/testfile.txt'
contact_file_fey = 'demo2/testfile.txt'

archive_bucket = ''
archive_key = 'processed_files/'

log_bucket =''
account_log = 'acclog/'
contacts_log = 'contlog/'

sf = Salesforce(username='username@gmail.com', \
    password='<password>', \
    security_token='<security-token>')


def process_accounts(**kwargs):

    s3Client=boto3.client('s3')
    response=s3Client.get_object(Bucket=s3_file_buckname,Key=account_file_fey)
    #Processit
    data=response['Body'].read().decode('utf-8')
    date_time_str = now.strftime("%Y%m%d%H%M%S")
    try:
        account=  list(csv.DictReader(io.StringI0(data)))
        sf.bulk.Account.upsert(account, 'Id', batch_size=10000, use_serial=True)
        print("Done. Accounts uploaded.")
        log = b'Done. Accounts uploaded.'
        s3Client.upload_fileobj(log, log_bucket, account_log+date_time_str+'.log')
    except Exception as e:
        print(e)
        log = bytes(e, "ascii")
        s3Client.upload_fileobj(log, log_bucket, account_log+date_time_str+'.log')
        raise AirflowFailException("Accounts upload to salesforce failed")

    

def process_contacts(**kwargs):
    
    s3Client=boto3.client('s3')
    response=s3Client.get_object(Bucket=s3_file_buckname,Key=contact_file_fey)
    #Processit
    data=response['Body'].read().decode('utf-8')
    
    date_time_str = now.strftime("%Y%m%d%H%M%S")
    try:
        Contact=list(csv.DictReader(io.StringI0(data)))
        sf.bulk.Contact.upsert(Contact, 'Id', batch_size=10000, use_serial=True)
        print("Done. Contact uploaded.")
        log = b'Done. Contact uploaded.'
        s3Client.upload_fileobj(log, log_bucket, contacts_log+date_time_str+'.log')

    except Exception as e:
        print(e)
        log = bytes(e, "ascii")
        s3Client.upload_fileobj(log, log_bucket, contacts_log+date_time_str+'.log')
        raise AirflowFailException("Accounts upload to salesforce failed")

def move_processed_files(**kwargs):
    date_time_str = now.strftime("%Y%m%d%H%M%S")
    s3Client=boto3.client('s3')
    copy_account = {
    'Bucket': s3_file_buckname,
    'Key': account_file_fey
    }

    copy_contact = {
    'Bucket': s3_file_buckname,
    'Key': contact_file_fey
    }
    s3Client.meta.client.copy(copy_account, archive_bucket, archive_key+date_time_str+'/')
    s3Client.delete_object(Bucket=s3_file_buckname, Key=account_file_fey)
    s3Client.meta.client.copy(copy_contact, archive_bucket, archive_key+date_time_str+'/')
    s3Client.delete_object(Bucket=s3_file_buckname, Key=contact_file_fey)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 11),
    'email_on_failure': False,
    'max_active_runs': 1,
    'email_on_retry': False,
}

dag = DAG('upload_salesforce_obj',
          schedule_interval='5 * * * *',
          default_args=default_args,
          catchup=False
          )


Account_file_check = S3KeySensor(
    task_id='Account_file_check',
    poke_interval=60,
    timeout=180,
    soft_fail=False,
    retries=2,
    bucket_key=account_file_fey,
    bucket_name=s3_file_buckname,
    aws_conn_id='aws_default',
    dag=dag)

Contact_file_check = S3KeySensor(
    task_id='Contact_file_check',
    poke_interval=60,
    timeout=180,
    soft_fail=False,
    retries=2,
    bucket_key=contact_file_fey,
    bucket_name=s3_file_buckname,
    aws_conn_id='aws_default',
    dag=dag)


upload_accounts = PythonOperator(
    task_id='upload_accounts',
    python_callable=process_accounts,
    dag=dag)

upload_contacts = PythonOperator(
    task_id='upload_contacts',
    python_callable=process_contacts,
    dag=dag)

move_to_archive = PythonOperator(
    task_id='move_to_archive',
    python_callable=move_processed_files,
    dag=dag)

Account_file_check >> upload_accounts >> upload_contacts

Contact_file_check >> upload_contacts >> move_to_archive