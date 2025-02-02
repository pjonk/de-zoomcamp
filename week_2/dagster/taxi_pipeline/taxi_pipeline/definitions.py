from dagster import Definitions, load_assets_from_modules
from dagster_gcp import GCSResource, BigQueryResource
from taxi_pipeline import assets 

from .config import settings

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "gcs": GCSResource(project=settings.project),
        "bigquery": BigQueryResource(project=settings.project)
    },
    )