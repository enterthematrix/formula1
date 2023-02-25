from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, FloatType

spark = SparkSession.builder.getOrCreate()

raw_dir_path = "/mnt/formula1spark/raw"
processed_dir_path = "/mnt/formula1spark/processed"
presentation_dir_path = "/mnt/formula1spark/presentation"


def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df


def ingest_circuits():
    circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                         StructField("circuitRef", StringType(), True),
                                         StructField("name", StringType(), True),
                                         StructField("location", StringType(), True),
                                         StructField("country", StringType(), True),
                                         StructField("lat", DoubleType(), True),
                                         StructField("lng", DoubleType(), True),
                                         StructField("alt", IntegerType(), True),
                                         StructField("url", StringType(), True)
                                         ])
    circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_dir_path}/circuits.csv")
    # drop URL col
    circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"),
                                              col("country"), col("lat"), col("lng"), col("alt"))
    # rename cols
    circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed(
        "circuitRef",
        "circuit_ref").withColumnRenamed(
        "lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude")
    # add ingestion_date col
    circuits_final_df = add_ingestion_date(circuits_renamed_df)
    circuits_final_df.write.mode("overwrite").parquet(f"{processed_dir_path}/circuits")
    # spark.read.parquet(f"{processed_dir_path}/circuits").show()


def ingest_races():
    races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("year", StringType(), True),
                                      StructField("round", StringType(), True),
                                      StructField("circuitId", IntegerType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("date", StringType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("url", StringType(), True)
                                      ])
    races_df = spark.read.option("header", True).schema(races_schema).csv(f"{raw_dir_path}/races.csv")
    races_selected_df = races_df.select(col("raceid").alias("race_id"), col("year").alias("race_year"), col("round"),
                                        col("circuitid").alias("circuit_id"), col("name"), col("date"), col("time"))

    races_ingestion_date_df = add_ingestion_date(races_selected_df)
    races_race_timestamp_df = races_ingestion_date_df.withColumn('race_timestamp', to_timestamp(
        concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
    races_final_df = races_race_timestamp_df.select(col("race_id"), col("race_year"),
                                                    col("round"),
                                                    col("circuit_id"), col("name"), col("race_timestamp"),
                                                    col('ingestion_date'))
    # races_final_df.show()
    races_final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_dir_path}/races")
    # spark.read.parquet(f"{processed_dir_path}/races").show()


def ingest_constructors():
    constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
    constructors_df = spark.read.schema(constructors_schema).json(f"{raw_dir_path}/constructors.json")
    # constructors_df.printSchema()
    constructors_drop_df = constructors_df.drop(col('url'))
    constructors_final_df = add_ingestion_date(constructors_drop_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed(
        "constructorRef", "constructor_ref"))

    constructors_final_df.write.mode("overwrite").parquet(f"{processed_dir_path}/constructors")
    # spark.read.parquet(f"{processed_dir_path}/constructors").show()


def ingest_drivers():
    name_schema = StructType(
        fields=[StructField("forename", StringType(), True), StructField("surname", StringType(), True)])
    driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                       StructField("driverRef", StringType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("code", StringType(), True),
                                       StructField("name", name_schema, True),
                                       StructField("dob", DateType(), True),
                                       StructField("nationality", StringType(), True),
                                       StructField("url", StringType(), True)
                                       ])
    drivers_df = spark.read.schema(driver_schema).json(f"{raw_dir_path}/drivers.json")
    drivers_with_col_df = add_ingestion_date(drivers_df.withColumnRenamed('driverId', 'driver_id')\
        .withColumnRenamed('driverRef','driver_ref').\
        withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))))
    drivers_df = drivers_with_col_df.drop(col('url'))
    drivers_df.write.mode("overwrite").parquet(f"{processed_dir_path}/drivers")
    # spark.read.parquet(f"{processed_dir_path}/drivers").show()


def ingest_results():
    results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("constructorId", IntegerType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("grid", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("positionText", StringType(), True),
                                        StructField("positionOrder", IntegerType(), True),
                                        StructField("points", FloatType(), True),
                                        StructField("laps", IntegerType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True),
                                        StructField("fastestLap", IntegerType(), True),
                                        StructField("rank", IntegerType(), True),
                                        StructField("fastestLapTime", StringType(), True),
                                        StructField("fastestLapSpeed", FloatType(), True),
                                        StructField("statusId", StringType(), True)])
    results_df = spark.read \
        .schema(results_schema) \
        .json(f"{raw_dir_path}/results.json")
    results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
        .withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("constructorId", "constructor_id") \
        .withColumnRenamed("positionText", "position_text") \
        .withColumnRenamed("positionOrder", "position_order") \
        .withColumnRenamed("fastestLap", "fastest_lap") \
        .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
        .withColumn("ingestion_date", current_timestamp())
    results_final_df = results_with_columns_df.drop(col("statusId"))
    results_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_dir_path}/results")
    # spark.read.parquet(f"{processed_dir_path}/results").show()


def ingest_pit_stops():
    pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                          StructField("driverId", IntegerType(), True),
                                          StructField("stop", StringType(), True),
                                          StructField("lap", IntegerType(), True),
                                          StructField("time", StringType(), True),
                                          StructField("duration", StringType(), True),
                                          StructField("milliseconds", IntegerType(), True)
                                          ])
    pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine", True).json(f"{raw_dir_path}/pit_stops.json")
    final_df = add_ingestion_date(pit_stops_df\
        .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("raceId", "race_id"))
    final_df.write.mode("overwrite").parquet(f"{processed_dir_path}/pit_stops")
    # spark.read.parquet(f"{processed_dir_path}/pit_stops").show()


def ingest_lap_times():
    lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                          StructField("driverId", IntegerType(), True),
                                          StructField("lap", IntegerType(), True),
                                          StructField("position", IntegerType(), True),
                                          StructField("time", StringType(), True),
                                          StructField("milliseconds", IntegerType(), True)
                                          ])
    lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_dir_path}/lap_times")
    final_df = add_ingestion_date(lap_times_df.withColumnRenamed("driverId", "driver_id")\
        .withColumnRenamed("raceId", "race_id"))
    final_df.write.mode("overwrite").parquet(f"{processed_dir_path}/lap_times")
    # spark.read.parquet(f"{processed_dir_path}/lap_times").show()


def ingest_qualifying():
    qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                           StructField("raceId", IntegerType(), True),
                                           StructField("driverId", IntegerType(), True),
                                           StructField("constructorId", IntegerType(), True),
                                           StructField("number", IntegerType(), True),
                                           StructField("position", IntegerType(), True),
                                           StructField("q1", StringType(), True),
                                           StructField("q2", StringType(), True),
                                           StructField("q3", StringType(), True),
                                           ])
    qualifying_df = spark.read \
        .schema(qualifying_schema) \
        .option("multiLine", True) \
        .json(f"{raw_dir_path}/qualifying")
    final_df = add_ingestion_date(qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
        .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("constructorId", "constructor_id"))

    final_df.write.mode("overwrite").parquet(f"{processed_dir_path}/qualifying")
    spark.read.parquet(f"{processed_dir_path}/qualifying").show()

# ingest_circuits()
# ingest_races()
# ingest_constructors()
# ingest_drivers()
# ingest_results()
# ingest_pit_stops()
# ingest_lap_times()
ingest_qualifying()
