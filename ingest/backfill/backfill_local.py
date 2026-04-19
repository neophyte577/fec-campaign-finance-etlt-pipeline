import concurrent.futures
import gc
import os
from pathlib import Path

import boto3
import pandas as pd
import polars as pl
import snowflake.connector

from dotenv import load_dotenv
load_dotenv()

import pyarrow.parquet as pq

DATASET = 'individual_contributions'
CYCLE = '2018'

DTYPE_MAPPING = {
    'VARCHAR': pl.Utf8,
    'STRING': pl.Utf8,
    'NUMBER': pl.Float64,
    'NUMERIC': pl.Float64,
    'INTEGER': pl.Int64,
    'INT': pl.Int64,
    'FLOAT': pl.Float64,
    'DECIMAL': pl.Float64,
    'BOOLEAN': pl.Boolean,
    'DATE': pl.Date,
    'TIMESTAMP': pl.Datetime,
    'TIME': pl.Time,
}

MB_THRESHOLD = 3072
S3_DIR = 'campaign-finance'
BUCKET_NAME = 'fec-data-staging-bucket'



def initialize_paths(dataset: str, cycle: str) -> dict[str, str]:
    raw_dir = Path('./raw')
    clean_dir = Path('./clean')
    raw_data_path = raw_dir / f'{dataset}_{cycle}.txt'
    cleaned_data_path = clean_dir / f'{dataset}_{cycle}_cleaned_data.txt'
    output_name = f'{dataset}_{cycle}.parquet'
    table_name = f'{dataset}_{cycle}'

    return {
        'dataset': dataset,
        'cycle': cycle,
        'raw_dir': str(raw_dir),
        'clean_dir': str(clean_dir),
        'raw_data_path': str(raw_data_path),
        'cleaned_data_path': str(cleaned_data_path),
        'output_name': output_name,
        'table_name': table_name,
        'output_path': str(clean_dir / output_name),
    }


def create_dirs(paths: dict[str, str]) -> None:
    os.makedirs(paths['clean_dir'], exist_ok=True)

    for file_path in [paths['cleaned_data_path'], paths['output_path']]:
        if os.path.exists(file_path):
            os.remove(file_path)


def preprocess(paths: dict[str, str]) -> None:
    print('Preprocessing...')
    raw_data_path = paths['raw_data_path']
    cleaned_data_path = paths['cleaned_data_path']

    if not os.path.exists(raw_data_path):
        raise FileNotFoundError(f'Raw file not found: {raw_data_path}')

    file_size = os.path.getsize(raw_data_path)
    file_size_mb = file_size / (1024 * 1024)

    if file_size_mb < MB_THRESHOLD:
        try:
            with open(raw_data_path, 'r', encoding='utf-8') as file:
                cleaned = file.read().replace(',|', '|').replace('"', '').replace("'", '')
        except UnicodeDecodeError:
            with open(raw_data_path, 'rb') as file:
                raw = file.read()
                cleaned = raw.decode('utf-8', errors='replace').replace('�', '')
                cleaned = cleaned.replace(',|', '|').replace('"', '').replace("'", '')
        with open(cleaned_data_path, 'w', encoding='utf-8') as cleaned_data:
            cleaned_data.write(cleaned)
    else:
        def clean_line(line: str) -> str:
            return line.replace(',|', '|').replace('"', '').replace("'", '')

        try:
            with open(raw_data_path, 'r', encoding='utf-8') as file, open(cleaned_data_path, 'w', encoding='utf-8') as cleaned_data:
                for line in file:
                    cleaned_data.write(clean_line(line))
        except UnicodeDecodeError:
            with open(raw_data_path, 'rb') as file, open(cleaned_data_path, 'w', encoding='utf-8') as cleaned_data:
                for raw_line in file:
                    line = raw_line.decode('utf-8', errors='replace').replace('�', '')
                    cleaned_data.write(clean_line(line))

    gc.collect()


def parse_data_type(data_type: str):
    data_type_upper = data_type.upper()
    if 'DATE' in data_type_upper:
        return pl.Utf8
    for sql_type in DTYPE_MAPPING:
        if sql_type in data_type_upper:
            return DTYPE_MAPPING[sql_type]
    return pl.Utf8


def write_parquet(paths: dict[str, str]) -> None:
    print('Writing to parquet...')
    schema_df = pd.read_csv(f'../metadata/schemas/{paths["dataset"]}.csv')
    header = list(schema_df['attribute'])
    dtype = {
        row['attribute']: parse_data_type(row['data_type'])
        for _, row in schema_df.iterrows()
    }

    cleaned_data_path = paths['cleaned_data_path']
    output_path = paths['output_path']

    file_size = os.path.getsize(cleaned_data_path)
    file_size_mb = file_size / (1024 * 1024)

    def convert_date_columns(df: pl.DataFrame, date_columns: list[str]) -> pl.DataFrame:
        for date_col in date_columns:
            if date_col in df.columns:
                df = df.with_columns(
                    pl.col(date_col).str.strptime(pl.Date, format='%m%d%Y', strict=False)
                )
        return df

    date_columns = [
        row['attribute']
        for _, row in schema_df.iterrows()
        if 'DATE' in row['data_type'].upper()
    ]

    if file_size_mb < MB_THRESHOLD:
        df = pl.read_csv(
            cleaned_data_path,
            separator='|',
            new_columns=header,
            schema_overrides=dtype,
            infer_schema_length=0,
            encoding='utf-8',
            ignore_errors=True,
        )
        df = convert_date_columns(df, date_columns)
        df.write_parquet(output_path)
    else:
        print('Big data time...')
        reader = pl.read_csv_batched(
            cleaned_data_path,
            separator='|',
            new_columns=header,
            schema_overrides=dtype,
            infer_schema_length=0,
            encoding='utf-8',
            ignore_errors=True,
            truncate_ragged_lines=True,
            batch_size=100_000,
        )

        writer = None

        try:
            iterations  = 0
            while True:
                iterations += 1
                batches = reader.next_batches(10)
                if not batches:
                    break

                for df in batches:
                    df = convert_date_columns(df, date_columns)
                    table = df.to_arrow()

                    if writer is None:
                        writer = pq.ParquetWriter(output_path, table.schema)

                    writer.write_table(table)

                gc.collect()

        finally:
            if writer is not None:
                print('Done writing')
                writer.close()


def upload_to_s3(paths: dict[str, str]) -> None:
    print('Uploading to S3...')
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        aws_session_token=os.environ.get('AWS_SESSION_TOKEN'),
        region_name=os.environ.get('AWS_DEFAULT_REGION'),
    )
    s3.upload_file(
        paths['output_path'],
        BUCKET_NAME,
        f"{S3_DIR}/{paths['cycle']}/{paths['output_name']}"
    )
    print('Conversion complete.')


def get_schema_dict() -> dict[str, pd.DataFrame]:
    schema_dir = '../metadata/schemas/'
    schemas = [file for file in os.listdir(schema_dir) if file.endswith('.csv')]
    schema_dict: dict[str, pd.DataFrame] = {}
    for file_name in schemas:
        dataset_name = file_name[:-4]
        schema_dict[dataset_name] = pd.read_csv(os.path.join(schema_dir, file_name))
    return schema_dict


def sql_query(schema_df: pd.DataFrame, table_name: str) -> str:
    sql = f'CREATE TABLE IF NOT EXISTS {table_name} (\n'
    for attribute in schema_df['attribute']:
        data_type = schema_df.loc[schema_df['attribute'] == attribute, 'data_type'].values[0]
        sql += f'    {attribute} {data_type},\n'
    sql = sql[:-2] + ');'
    return sql


def get_snowflake_connection():
    print('Connecting to Snowflake...')
    return snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PAT'],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'fec_wh'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'fec'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA', 'raw'),
    )


def truncate_table(paths: dict[str, str]) -> None:
    truncate_table_sql = f"TRUNCATE TABLE IF EXISTS fec.raw.{paths['table_name']};"
    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(truncate_table_sql)


def create_table(paths: dict[str, str], schema_dict: dict[str, pd.DataFrame]) -> None:
    print('Building table in Snowflake...')
    schema_df = schema_dict[paths['dataset']]
    create_table_sql = sql_query(schema_df, paths['table_name'])
    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)


def load_data(paths: dict[str, str]) -> None:
    print('Loading data into Snowflake...')
    copy_sql = f"""
    COPY INTO fec.raw.{paths['table_name']}
        FROM @FEC.RAW.S3_STAGE/campaign-finance/{paths['cycle']}/{paths['output_name']}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE';
    """
    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(copy_sql)
    print('Loading complete.')


def main() -> None:
    print('In main')
    paths = initialize_paths(DATASET, CYCLE)
    schema_dict = get_schema_dict()

    create_dirs(paths)
    preprocess(paths)
    write_parquet(paths)
    upload_to_s3(paths)
    truncate_table(paths)
    create_table(paths, schema_dict)
    load_data(paths)


if __name__ == '__main__':
    
    main()
    print('\nFin')
