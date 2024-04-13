from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import SparkSession
import mlflow

# Set MLflow tracking URI (assuming your MLflow server is running at http://127.0.0.1:5005)
mlflow.set_tracking_uri("http://spark-master:5005")

spark = (
    SparkSession.builder
    .config("spark.jars.packages", "org.mlflow:mlflow-spark:2.22.0")
    .master("local[*]")
    .getOrCreate()
)

# Create training data
training = spark.createDataFrame(
    [
        (0, "a b c d e spark", 1.0),
        (1, "b d", 0.0),
        (2, "spark f g h", 1.0),
        (3, "hadoop mapreduce", 0.0),
    ],
    ["id", "text", "label"],
)

# Define preprocessing and model stages
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.001)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# Train the model
model = pipeline.fit(training)

# Experiment name (replace with your desired name)
experiment_name = "spark_classification_exp"

try:
    # Attempt to create the experiment if it doesn't exist (MLflow >= 1.9.0)
    mlflow.create_experiment(experiment_name)

except:
  # Handle potential exception if experiment already exists (older MLflow versions)
  print("Experiment with name '{}' already exists".format(experiment_name))

current_experiment=dict(mlflow.get_experiment_by_name(experiment_name))
print(current_experiment)
exp_id=current_experiment['experiment_id']
# Log the model to the created experiment
with mlflow.start_run(experiment_id=exp_id) as run:
    mlflow.spark.log_model(model, "spark-model")
# (Optional) Stop the SparkSession (if not needed anymore)
spark.stop()
