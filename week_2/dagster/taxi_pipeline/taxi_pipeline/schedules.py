from dagster import ScheduleDefinition
from jobs import taxi_update_job

trip_update_schedule = ScheduleDefinition(
    job=taxi_update_job,
    cron_schedule="0 0 5 * *", 
)