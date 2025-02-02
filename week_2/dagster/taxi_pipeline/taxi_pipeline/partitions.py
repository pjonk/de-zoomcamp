from dagster import MonthlyPartitionsDefinition

start_date = "2020-01-01"
end_date = "2021-08-01"

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)
