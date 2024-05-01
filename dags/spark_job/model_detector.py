from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, cast
from feature_builder import build_predicting_feature
from datetime import datetime
import mlflow
import argparse
import pandas


def parse_arguments():
    """
    Parses arguments passed through spark-submit

    Returns:
        Namespace: An object containing parsed arguments
    """
    parser = argparse.ArgumentParser(description="Landslide detection model predictor")
    parser.add_argument("--region", type=str, help="Specific model for prediction depending on region")
    parser.add_argument("--run_id", type=str, help="Specific run id for prediction")
    return parser.parse_args()

def main(spark):
    # mlflow.set_tracking_uri("http://spark-master:5005")
    if args.region == "america":
        in_path = "/opt/airflow/buffer/message_broker/outline_america.csv"
    elif args.region == "northern_asia":
        in_path = "/opt/airflow/buffer/message_broker/outline_northern_asia.csv"
    else:
        in_path = "/opt/airflow/buffer/message_broker/outline_data.csv"

    prepared_df = spark.read.format("csv") \
        .option("header", True)  \
        .option("inferSchema", True)  \
        .load(in_path)
    # prepared_df.show()
    # prepared_df.printSchema()
    metadata_df = prepared_df.groupBy("Index","longitude_info","latitude_info","date","year","month","elevation","time")\
                            .agg({"temperature_2m":"avg","rain":"avg","precipitation":"avg","relative_humidity_2m":"avg","soil_temperature_0_to_7cm":"avg","soil_temperature_7_to_28cm":"avg","soil_temperature_28_to_100cm":"avg","soil_temperature_100_to_255cm":"avg",\
                                "soil_moisture_0_to_7cm":"avg","soil_moisture_7_to_28cm":"avg","soil_moisture_28_to_100cm":"avg","soil_moisture_100_to_255cm":"avg"})\
                            .select("Index","longitude_info","latitude_info","date","year","month","elevation","time")
    # metadata_df.show()
    predict(build_feature(prepared_df),metadata_df)
    # print(prediction, type(prediction))


def build_feature(prepared_df):
    feature_df = build_predicting_feature(prepared_df)
    # feature_df.show()
    # feature_df.printSchema()
    assembler = VectorAssembler(inputCols=["features_temperature", "features_rain", "features_precipitation", "features_relative_humidity",\
                                           "features_soil_temp_0_to_7", "features_soil_temp_7_to_28", "features_soil_temp_28_to_100", "features_soil_temp_100_to_255", \
                                            "features_soil_moisture_0_to_7", "features_soil_moisture_7_to_28", "features_soil_moisture_28_to_100", "features_soil_moisture_100_to_255"],\
                                outputCol="features")
    feature_df = assembler.transform(feature_df)
    feature_df_pandas = feature_df.toPandas()
    return feature_df_pandas

def predict(feature_df_pandas,metadata_df):
    logged_model = '/mlartifacts/{}/artifacts/model'.format(args.run_id)
    # out_path = '/opt/airflow/buffer/destination/predicting_{}.csv'.format(datetime.now().strftime("%Y%m%d%H%M%S"))
    if args.region == "america":
    #     logged_model = 'models:/detector_region_2/3'
        out_path = '/opt/airflow/buffer/destination/predicting_america_{}.csv'.format(datetime.now().strftime("%Y%m%d%H%M%S"))
    elif args.region == "northern_asia":
    #     logged_model = 'models:/detector_region_1/3'
        out_path = '/opt/airflow/buffer/destination/predicting_northern_asia_{}.csv'.format(datetime.now().strftime("%Y%m%d%H%M%S"))
    else:
    #     logged_model = 'models:/detector_region_1/1'
        out_path = '/opt/airflow/buffer/destination/predicting_{}.csv'.format(datetime.now().strftime("%Y%m%d%H%M%S"))

    loaded_model = mlflow.pyfunc.load_model(logged_model)
    prediction = loaded_model.predict(feature_df_pandas)
    result_df = feature_df_pandas.assign(prediction=prediction)
    metadata_df = metadata_df.withColumn("time", metadata_df["time"].cast("string"))  # Convert to string temporarily
    metadata_df = metadata_df.toPandas()
    metadata_df["time"] = pandas.to_datetime(metadata_df["time"])
    result_df = result_df.set_index('Index').join(metadata_df.set_index('Index'), how='outer')
    result_df.to_csv(out_path, index=True)

if __name__ == "__main__":
    # Parse arguments
    args = parse_arguments()
    # Build spark session
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('Detector') \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.3.9') \
        .config("spark.jars.packages", "org.mlflow:mlflow-spark:2.11.3") \
        .getOrCreate()

    main(spark)

    spark.stop()
