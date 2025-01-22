from airflow.decorators import task
from region_initial_etl.utils import set_filtering_date, filter_by_date_region, upload_to_s3

@task
def process_region_data():
    # weekly_dict = set_filtering_date(start_date="2022-01-02", end_date="2023-06-03", freq="7D")
    weekly_dict = set_filtering_date(start_date="2022-01-02", end_date="2023-06-03", freq="7D")
    for week, date_range in weekly_dict.items():
        start_date = date_range["start_date"]
        end_date = date_range["end_date"]

        # Filter data for the week
        region_data = filter_by_date_region("aihub", "2022", start_date, end_date)

        # Upload filtered data to S3
        if region_data:
            upload_to_s3(region_data, start_date)
