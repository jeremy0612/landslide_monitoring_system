from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Initialize the spark job
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('MyApp') \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', 'mongodb://admin:password@mongo/stackoverflow?authSource=admin') \
        .config('spark.mongodb.output.uri', 'mongodb://admin:password@mongo/stackoverflow?authSource=admin') \
        .getOrCreate()

    

