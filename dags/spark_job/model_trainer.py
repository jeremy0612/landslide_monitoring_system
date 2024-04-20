from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
# from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from feature_builder import build_feature
import argparse
import mlflow
import sys
import os

def parse_arguments():
  """
  Parses arguments passed through spark-submit

  Returns:
      Namespace: An object containing parsed arguments
  """
  parser = argparse.ArgumentParser(description="Landslide detection model trainer")
  parser.add_argument("--master", type=str, help="Spark master URL")
  parser.add_argument("--name", type=str, help="Spark application name")
  parser.add_argument("--experiment_name", type=str, default="landslide_detection", help="MLflow experiment name")
  parser.add_argument("--experiment_id", type=str, default=None, help="MLflow experiment ID (optional)")
  parser.add_argument("--model_name", type=str, help="Name to register the model in MLflow")
  return parser.parse_args()

def main(spark, experiment_name, experiment_id, model_name):
    # Set MLflow tracking URI (assuming your MLflow server is running at http://127.0.0.1:5005)
    mlflow.set_tracking_uri("http://spark-master:5005")
    mlflow.pyspark.ml.autolog(spark)
    # mlflow.spark.autolog()
    # Read data from Postgres
    landslide_event_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/landslide_warehouse") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "landslide_event") \
        .option("user", "admin") \
        .option("password", "123456") \
        .load()

    location_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/landslide_warehouse") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "location") \
        .option("user", "admin") \
        .option("password", "123456") \
        .load()
    
    datetime_df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://postgres:5432/landslide_warehouse")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "datetime")\
        .option("user", "admin")\
        .option("password", "123456")\
        .load()
    
    soil_df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://postgres:5432/landslide_warehouse")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "soil")\
        .option("user", "admin")\
        .option("password", "123456")\
        .load()
    
    weather_data_df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://postgres:5432/landslide_warehouse")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "weather_data")\
        .option("user", "admin")\
        .option("password", "123456")\
        .load()
    
    # Join dataframes on location_id
    # df = location_df.join(landslide_event_df, on='location_id', how='inner')
    # df = df.join(weather_df, on='location_id', how='inner')
    
    # Define a function to filter data based on continent
    def filter_by_region(df, min_longitude, max_longitude, min_latitude, max_latitude, label):
        return df.filter((df.longitude > min_longitude) & 
                        (df.longitude < max_longitude) &
                        (df.latitude < max_latitude) &
                        (df.latitude > min_latitude)) 

    # Prepare data for Asia and America with labels
    assembler = VectorAssembler(inputCols=["features_temperature", "features_rain", "features_precipitation", "features_relative_humidity",\
                                           "features_soil_temp_0_to_7", "features_soil_temp_7_to_28", "features_soil_temp_28_to_100", "features_soil_temp_100_to_255", \
                                            "features_soil_moisture_0_to_7", "features_soil_moisture_7_to_28", "features_soil_moisture_28_to_100", "features_soil_moisture_100_to_255"],\
                                outputCol="features")
    feature_df = build_feature(soil_df, weather_data_df, datetime_df, landslide_event_df)
    feature_df = assembler.transform(feature_df)

    # Branching by region 
    event_df = location_df.join(landslide_event_df,on=[location_df['location_id']==landslide_event_df['location_id']])\
                            .select('longitude','latitude','event_id')
    feature_df = feature_df.join(event_df,on=[event_df['event_id']==feature_df['event_id']])

    if model_name == "detector_region_1":
        train_df = filter_by_region(feature_df, 92, 150, -1, 25, 1)
    elif model_name == "detector_region_2":
        train_df = filter_by_region(feature_df, -1, 54, -83, 56, 0)
        
    train(train_df)

    # asia_df = assembler.transform(filter_by_continent(df, 92, 150, -1, 25, 1).withColumn("region", lit('AS')))
    # america_df = assembler.transform(filter_by_continent(df, -1, 54, -83, 56, 0).withColumn("region", lit('US')))

    # # Combine DataFrames for training
    # combined_df = asia_df.union(america_df)

    
    spark.stop()

def train(feature_df):
    train_data, test_data = feature_df.randomSplit([0.8, 0.2], seed=42)
    # Check DataFrame sizes after splitting
    print(f"Train data size: {train_data.count()}")
    print(f"Test data size: {test_data.count()}")

    # Check for missing features after transformations
    print(f"Train data features: {train_data.schema.fields}")
    print(f"Test data features: {test_data.schema.fields}")
    # Define and log model parameters
    model_params = {
        "maxIter": 10,
        "regParam": 0.3,
        "elasticNetParam": 0.8
    }
    
    # Start MLflow run with experiment ID and name (optional)
    with mlflow.start_run(experiment_id=experiment_id) as run:
        # Train the Logistic Regression model
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=model_params["maxIter"], regParam=model_params["regParam"], elasticNetParam=model_params["elasticNetParam"])
        pipeline = Pipeline(stages=[lr])
        model = pipeline.fit(train_data)
        # Log the model as an artifact
        mlflow.spark.log_model(spark_model=model, artifact_path="model")
        # Register the model (optional, but recommended) 
        mlflow.register_model(model_uri=f"runs/{run.info.run_id}/model", name=model_name)

        run_id = run.info.run_id
    # Ensure the directory exists before opening the file
    if os.path.exists('/opt/airflow/buffer/mlflow/'):
        with open('/opt/airflow/buffer/mlflow/' + model_name + '.txt', 'w') as outfile:
            outfile.write(run_id)
    else:
        os.makedirs('/opt/airflow/buffer/mlflow/', exist_ok=True)  # Create directories recursively, ignoring already existing ones
        with open('/opt/airflow/buffer/mlflow/' + model_name + '.txt', 'w') as outfile:
                outfile.write(run_id)
        
    pass

if __name__ == "__main__":
    # Parse arguments
    args = parse_arguments()
    # Initialize the Spark session
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('Trainer') \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.3.9') \
        .config("spark.jars.packages", "org.mlflow:mlflow-spark:2.11.3") \
        .getOrCreate()
    # Use other arguments in your script
    experiment_name = args.experiment_name
    experiment_id = args.experiment_id
    model_name = args.model_name
    # print("Experiment name: ", experiment_name)
    # print("Experiment id: ", experiment_id)
    # print("Model name: ", model_name)
    # sys.exit(1)
    if experiment_id is None:
        current_experiment=dict(mlflow.get_experiment_by_name(experiment_name))
        print("Current_experiment: ",current_experiment)
        experiment_id=current_experiment['experiment_id']
        print("Experiment id not found or not created yet !!!")
        sys.exit(1)

    main(spark, experiment_name, experiment_id, model_name)
    


    
    
    