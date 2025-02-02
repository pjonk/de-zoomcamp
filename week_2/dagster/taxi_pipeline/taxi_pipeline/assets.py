import pandas as pd
import dagster as dg
from dagster_gcp import GCSResource, BigQueryResource
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from io import BytesIO

from .partitions import monthly_partition
from .config import settings

def load_taxi_data(context: dg.AssetExecutionContext, year_month: str, color: str):

    def get_url(year_month: str):
        return f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year_month}.csv.gz' 

    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float,
                    'ehail_fee':float
                }

    # native date parsing 
    if color == "green":
        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    elif color == "yellow":
        parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    context.log.info(f"Processing {get_url(year_month)}")

    df = pd.read_csv(get_url(year_month), sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
    df['source_year_month'] = year_month
    df['source_year_month'] = pd.to_datetime(df['source_year_month'], format="%Y-%m")

    return df


@dg.asset(
        deps=['taxi_bucket_data'],
        partitions_def=monthly_partition
)
def taxi_bigquery_data(context: dg.AssetExecutionContext, bigquery: BigQueryResource, gcs: GCSResource):
    """ Load taxi data into bigquery """

    period_to_fetch = context.partition_key
    year_month = period_to_fetch[:7] 

    color = settings.color
    bucket_name = settings.bucket
    destination_blob_name = f"data/{color}_taxi_data_{year_month}.parquet"

    # Get GCS client and download the Parquet file
    client = gcs.get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(destination_blob_name)

    if not blob:
        raise ValueError(f"Blob {destination_blob_name} not found in bucket {bucket_name}")

    parquet_data = blob.download_as_bytes()
    client.close()

    # Load Parquet file into a Pandas DataFrame
    df = pd.read_parquet(BytesIO(parquet_data))

    # BigQuery table details
    table_id = f"taxi_data.{color}_taxi_data"

    # Configure job to append data instead of overwriting
    job_config = LoadJobConfig(
        write_disposition=WriteDisposition.WRITE_APPEND
    )

    # Upload DataFrame to BigQuery
    with bigquery.get_client() as client:
        job = client.load_table_from_dataframe(
            dataframe=df,
            destination=table_id,
            job_config=job_config
        )
        job.result()  

    return dg.MaterializeResult(
        metadata={
            'Number of records': dg.MetadataValue.int(len(df.index))
        }
    )


@dg.asset(
        partitions_def=monthly_partition
)
def taxi_bucket_data(context: dg.AssetExecutionContext, gcs: GCSResource):
    """Load taxi data into a GCS bucket. """

    # Get partition info.
    period_to_fetch = context.partition_key
    year_month = period_to_fetch[:7] 

    # Define GCS details
    color = settings.color
    bucket_name = settings.bucket
    destination_blob_name = f"data/{color}_taxi_data_{year_month}.parquet"

    # Load taxi data into a Pandas DataFrame
    results = load_taxi_data(context, year_month, color)

    # Convert DataFrame to Parquet format in memory
    buffer = BytesIO()
    results.to_parquet(buffer, index=False)
    buffer.seek(0)  # Move cursor to the start of the buffer

    # Upload to GCS
    client = gcs.get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    context.log.info(f"Uploading to GCS blob: {destination_blob_name}")
    blob.upload_from_file(buffer, content_type="application/octet-stream")

    client.close()

    uncompressed_size = results.memory_usage(deep=True).sum()

    return dg.MaterializeResult(
        metadata={
            'Number of records': dg.MetadataValue.int(len(results.index)),
            "Estimated uncompressed size (MB)": dg.MetadataValue.md(f" {uncompressed_size / (1024 * 1024):.2f}")
        } 
    )

