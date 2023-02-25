from pyspark.sql.functions import current_timestamp

raw_dir_path = "/mnt/formula1spark/raw"
processed_dir_path = "/mnt/formula1spark/processed"
presentation_dir_path = "/mnt/formula1spark/presentation"


def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df
