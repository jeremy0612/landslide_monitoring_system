import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
# from torchviz import make_dot
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from feature_builder import build_training_feature
from datetime import datetime
from model import landslide_identification
from mlflow.models import infer_signature
import mlflow
import argparse
import numpy as np

def parse_arguments():
    parser = argparse.ArgumentParser(description="Landslide detection model trainer")
    parser.add_argument("--master", type=str, help="Spark master URL")
    parser.add_argument("--name", type=str, help="Spark application name")
    parser.add_argument("--experiment_name", type=str, default="landslide_detection", help="MLflow experiment name")
    parser.add_argument("--experiment_id", type=str, default=None, help="MLflow experiment ID (optional)")
    parser.add_argument("--model_name", type=str, help="Name to register the model in MLflow")
    return parser.parse_args()

def main(spark, experiment_name, experiment_id, model_name, num_epochs):
    mlflow.set_tracking_uri("http://spark-master:5005")
    mlflow.pyspark.ml.autolog(spark)
    # mlflow.pytorch.autolog(log_models=True, log_datasets=True)

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
    
    def filter_by_region(df, min_longitude, max_longitude, min_latitude, max_latitude):
        return df.filter((df.longitude > min_longitude) & 
                        (df.longitude < max_longitude) &
                        (df.latitude < max_latitude) &
                        (df.latitude > min_latitude)) 

    assembler = VectorAssembler(inputCols=["features_temperature", "features_rain", "features_precipitation", "features_relative_humidity",\
                                           "features_soil_temp_0_to_7", "features_soil_temp_7_to_28", "features_soil_temp_28_to_100", "features_soil_temp_100_to_255", \
                                            "features_soil_moisture_0_to_7", "features_soil_moisture_7_to_28", "features_soil_moisture_28_to_100", "features_soil_moisture_100_to_255"],\
                                outputCol="features")
    # assembler = VectorAssembler(inputCols=["features_temperature", "features_rain", "features_precipitation", "features_relative_humidity",\
    #                                         "features_soil_temp_100_to_255", \
    #                                         "features_soil_moisture_100_to_255"],\
    #                             outputCol="features")
        
    feature_df = build_training_feature(soil_df, weather_data_df, datetime_df, landslide_event_df)
    feature_df = assembler.transform(feature_df)

    event_df = location_df.join(landslide_event_df, on=[location_df['location_id'] == landslide_event_df['location_id']]) \
                          .select('longitude', 'latitude', 'event_id')
    feature_df = feature_df.join(event_df, on=[event_df['event_id'] == feature_df['event_id']])

    if model_name == "detector_region_1":
        train_df = filter_by_region(feature_df, 70, 160, -13, 25)
    elif model_name == "detector_region_2":
        train_df = filter_by_region(feature_df, -130, -50, 0, 50)
    
    train_df = train_df.select("features", "label")
        
    train(train_df, experiment_id, model_name, num_epochs)

def prepare_data(feature_df):
    train_data, val_data = feature_df.randomSplit([0.8, 0.2], seed=42)

    def df_to_tensor(df):
        data = df.select("features", "label").collect()
        X = torch.tensor([row["features"] for row in data], dtype=torch.float32)
        y = torch.tensor([row["label"] for row in data], dtype=torch.float32)  # Ensure labels are long integers for CrossEntropyLoss
        return TensorDataset(X, y)

    train_dataset = df_to_tensor(train_data)
    val_dataset = df_to_tensor(val_data)
    
    input_data = train_dataset[0][0]
    output_data = train_dataset[0][1]

    print("Input data:", input_data)
    print("Input data type:", type(input_data))
    print("Input data shape:", input_data.shape[0] if isinstance(input_data, (np.ndarray, torch.Tensor)) else "Not a tensor/array")
    
    print("Output data:", output_data)
    print("Output data type:", type(output_data))
    print("Output data shape:", output_data.shape if isinstance(output_data, (np.ndarray, torch.Tensor)) else "Not a tensor/array")
    
    # Infer signature
    # signature = infer_signature(input_data.numpy(), output_data.numpy())

    input_dim = train_dataset[0][0].shape[0]
    # print("debug ",input_dim)
    # sys.exit()
    output_dim = 1  # Assuming binary classification with two classes
    train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=32, shuffle=False)
    return train_loader, val_loader, input_dim, output_dim

def train(feature_df, experiment_id, model_name, num_epochs):
    train_loader, val_loader, input_dim, output_dim = prepare_data(feature_df)
    
    model = landslide_identification(input_dim, output_dim)
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.0001)
    scheduler = optim.lr_scheduler.OneCycleLR(
        optimizer,
        max_lr=0.001,
        steps_per_epoch=int(len(train_loader)),
        epochs=num_epochs,
        anneal_strategy="linear",
    )

    def train_epoch(model, loader, criterion, optimizer):
        model.train()
        running_loss = 0.0
        correct_predictions = 0
        total_predictions = 0
        for inputs, labels in loader:
            optimizer.zero_grad()
            outputs = model(inputs).float()
            # print(outputs.shape,outputs.squeeze(1).float().shape, labels.shape)
            loss = criterion(outputs, labels.unsqueeze(1))
            loss.backward()
            optimizer.step()
            scheduler.step()
            running_loss += loss.item() * inputs.size(0)
            preds = torch.argmax(outputs, dim=None)
            correct_predictions += (preds == labels).sum().item()
            total_predictions += labels.size(0)
        accuracy = correct_predictions / total_predictions
        return running_loss / len(loader.dataset), accuracy

    def evaluate(model, loader, criterion):
        model.eval()
        running_loss = 0.0
        correct_predictions = 0
        total_predictions = 0
        with torch.no_grad():
            for inputs, labels in loader:
                outputs = model(inputs).float()
                loss = criterion(outputs, labels.unsqueeze(1))
                running_loss += loss.item() * inputs.size(0)
                preds = torch.argmax(outputs, dim=None)
                correct_predictions += (preds == labels).sum().item()
                total_predictions += labels.size(0)
        accuracy = correct_predictions / total_predictions
        return running_loss / len(loader.dataset), accuracy

    with mlflow.start_run(experiment_id=experiment_id,\
                          #log_system_metrics=True,\
                          run_name=f"{model_name}_{datetime.now().strftime('%Y%m%d')}"):
        # mlflow.log_input(train_dataset, context="train_dataset")
        # mlflow.log_input(val_dataset, context="val_dataset")
        mlflow.log_param("learning_rate", optimizer.param_groups[0]["lr"])
        mlflow.log_param("optimizer", optimizer.__class__.__name__)
        mlflow.log_param("scheduler", scheduler.__class__.__name__)
        for epoch in range(num_epochs):
            train_loss, train_accuracy = train_epoch(model, train_loader, criterion, optimizer)
            val_loss, val_accuracy = evaluate(model, val_loader, criterion)

            print(f"Epoch {epoch + 1}/{num_epochs}, Train Loss: {train_loss:.4f}, Train Accuracy: {train_accuracy:.4f}, Val Loss: {val_loss:.4f}, Val Accuracy: {val_accuracy:.4f}")
            mlflow.log_metric("train_loss", train_loss, step=epoch)
            mlflow.log_metric("train_accuracy", train_accuracy, step=epoch)
            mlflow.log_metric("val_loss", val_loss, step=epoch)
            mlflow.log_metric("val_accuracy", val_accuracy, step=epoch)



        # torch.save(model.state_dict(), "landslide_identification_model.pth")
        # mlflow.log_artifact("landslide_identification_model.pth")
        mlflow.pytorch.log_model(model,artifact_path="model")

if __name__ == "__main__":
    args = parse_arguments()
    spark = SparkSession.builder.master('local').appName('Trainer') \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.3.9') \
        .config("spark.jars.packages", "org.mlflow:mlflow-spark:2.11.3") \
        .getOrCreate()

    experiment_name = args.experiment_name
    experiment_id = args.experiment_id
    model_name = args.model_name
    num_epochs = 100

    if experiment_id is None:
        current_experiment = dict(mlflow.get_experiment_by_name(experiment_name))
        experiment_id = current_experiment['experiment_id']

    main(spark, experiment_name, experiment_id, model_name, num_epochs)
    spark.stop()
