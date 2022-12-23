import os
import shutil
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, NoRegionError
from dotenv import load_dotenv
from loguru import logger

workdir = Path(__file__).resolve().parent.parent
query_path = workdir / 'query'

# Load environment variables
load_dotenv()

logger.add(
    sys.stdout,
    level='INFO',
    format='{message} - line: {line} ',
    colorize=True,
    backtrace=True,
    diagnose=True,
)

try:
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET'),
    )   # cria uma sessão com as credenciais do S3

    conn = os.getenv('CON')
    s3 = session.resource('s3')   # cria a sessão com o S3

    bucket = s3.Bucket(os.getenv('BUCKET'))   # seleciona o bucket

except NoCredentialsError:
    logger.error('No AWS Credentials')

s3_query = os.getenv('S3_QUERY')
list_query_s3 = [_ for _ in bucket.objects.filter(Prefix=f'{s3_query}/')]

s3_data_csv = os.getenv('S3_DATA_UNLOAD')
list_data_csv = [_ for _ in bucket.objects.filter(Prefix=f'{s3_data_csv}/')]

conn = create_engine(
    conn,
    echo=True,
)

query_path.mkdir(parents=True, exist_ok=True)


def delete_data_s3() -> None:

    """Delete data in S3."""

    try:
        if s3_data_csv != []:
            for data_csv in list_data_csv:
                bucket.delete_objects(
                    Delete={
                        'Objects': [
                            {
                                'Key': data_csv.key,
                            },
                        ],
                    },
                )
                logger.info(f'File {data_csv.key} deleted')
        else:
            logger.info('No data in S3')
            pass
    except ClientError as e:
        logger.error(e)
        pass


def execute_unload() -> None:

    """Executa o unload do Salesforce."""

    for query_s3 in list_query_s3:
        name_data_file = f'{query_path}/{query_s3.key.split("/")[-1]}'
        bucket.download_file(query_s3.key, name_data_file)
        logger.info(f'File {query_s3.key} downloaded')

    for sql_txt in query_path.iterdir():

        sql_txt_file = sql_txt.read_text()
        slq_txt_name = str(sql_txt).split('/')[-1].replace('.txt', '')

        query = sql_txt_file.replace("'", "''")

        unload = f"""UNLOAD ('{query}') to 's3://bucket/upload-dir/{slq_txt_name}_'
                iam_role 'arn:aws:iam::000000000000:role/role_name'
                delimiter '|' HEADER region '' format as csv parallel false;
                """
        try:
            conn.execute(text(unload).execution_options(autocommit=True))
            logger.info(f'Query {slq_txt_name} executed')
        except Exception as e:
            logger.error(e)
            pass

    shutil.rmtree(query_path, ignore_errors=True)


if __name__ == '__main__':
    delete_data_s3()
    execute_unload()
