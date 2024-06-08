from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, cast
from torch.utils.data import TensorDataset
from feature_builder import build_predicting_feature
from datetime import datetime
import mlflow
import argparse
import pandas
import numpy
import torch 

def parse_arguments():
    """
    Parses arguments passed through spark-submit

    Returns:
        Namespace: An object containing parsed arguments
    """
    parser = argparse.ArgumentParser(description="Landslide detection model predictor")
    parser.add_argument("--region", type=str, help="Specific model for prediction depending on region")
    parser.add_argument("--artifact_path", type=str, help="Specific artifact location of the respective model")
    return parser.parse_args()

def main(spark):
    # mlflow.set_tracking_uri("http://spark-master:5005")
    if args.region == "america":
        in_path = "/opt/airflow/buffer/message_broker/outline_america.csv"
        # in_path = "/usr/local/share/buffer/message_broker/outline_america.csv"
    elif args.region == "northern_asia":
        in_path = "/opt/airflow/buffer/message_broker/outline_northern_asia.csv"
        # in_path = "/usr/local/share/buffer/message_broker/outline_northern_asia.csv"
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
    result_df = spark.createDataFrame(predict(build_feature(prepared_df)))
    result_df = result_df.join(metadata_df, on="Index")
    # result_df = result_df.toPandas()
    # result_df.info()
    # result_df.to_csv('/opt/airflow/buffer/destination/inference.csv', index=False)
    result_df.printSchema()
    result_df.write.format("jdbc")\
        .option("url", "jdbc:postgresql://postgres:5432/landslide_warehouse")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "inference")\
        .option("user", "admin")\
        .option("password", "123456")\
        .mode("append")\
        .save()


def build_feature(prepared_df):
    feature_df = build_predicting_feature(prepared_df)
    # feature_df.show()
    # feature_df.printSchema()
    assembler = VectorAssembler(inputCols=["features_temperature", "features_rain", "features_precipitation", "features_relative_humidity",\
                                           "features_soil_temp_0_to_7", "features_soil_temp_7_to_28", "features_soil_temp_28_to_100", "features_soil_temp_100_to_255", \
                                            "features_soil_moisture_0_to_7", "features_soil_moisture_7_to_28", "features_soil_moisture_28_to_100", "features_soil_moisture_100_to_255"],\
                                outputCol="features")
    # zero_value = F.lit(0)
    # feature_df = feature_df.withColumn("label",zero_value)
    feature_df = assembler.transform(feature_df)
    feature_df = feature_df.select("features","Index")
    feature_df.show()
    print(feature_df[0])
    feature_df.printSchema()
    # feature_df_pandas = feature_df.toPandas()
    data = feature_df.select("features","Index").collect()
    temp = [numpy.array(row["features"]).reshape(1,288) for row in data]
    # print(temp)
    X = torch.tensor(temp,dtype=torch.float32)
    y = torch.tensor([row["Index"] for row in data],dtype=torch.long)
    feature_df_torch = TensorDataset(X, y)
    
    features = []
    indexes = []
    for sample in feature_df_torch:
        print(sample[0].shape)
        features.append(sample[0].numpy())
        indexes.append(sample[1].numpy())

    feature = pandas.DataFrame()
    feature['Features'] = features
    feature['Index'] = indexes
    
    return feature

def predict(feature_df_pandas):
    logged_model = args.artifact_path
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

    predictions = pandas.DataFrame()
    for row in feature_df_pandas.itertuples():
        # print(row)
        prediction = loaded_model.predict(row.Features)
        predictions = predictions.append({'Index': row.Index, 'prediction': prediction}, ignore_index=True)

    print('prediction: \n', predictions)
    print('shape: ', predictions.shape)
    print('type(prediction): ', type(predictions))
    predictions['prediction'] = predictions['prediction'].apply(lambda x: x[0][0])
    # predictions['prediction'] = predictions['prediction'].iloc[:, 0]
    # print('inference: ', predictions['prediction'].iloc[0][0][0], type(predictions['prediction'].iloc[0][0][0]))
    return predictions


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
