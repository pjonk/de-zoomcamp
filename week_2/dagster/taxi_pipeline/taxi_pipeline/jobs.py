from dagster import define_asset_job, AssetSelection

from partitions import monthly_partition


taxi_update_job = define_asset_job(
    name="taxi_update_job",
    partitions_def=monthly_partition,
    selection=AssetSelection.all(),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1,  # limits concurrent assets to 1
                },
            }
        }
    },
)
