from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json 

if __name__ == "__main__":
    # Initialize the spark job
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('Sample') \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.3.9') \
        .config('spark.jars', '/opt/airflow/dags/spark_job/postgresql-42.3.9.jar') \
        .getOrCreate()

    landslide_event_df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://postgres:5432/landslide_warehouse")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "landslide_event")\
        .option("user", "admin")\
        .option("password", "123456")\
        .load()
    
    location_df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://postgres:5432/landslide_warehouse")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "location")\
        .option("user", "admin")\
        .option("password", "123456")\
        .load()
    
    df = location_df.join(landslide_event_df, on='location_id', how='inner')
    
    # df.printSchema()

    # Select relevant columns (latitude, longitude)
    # Define a lower precision for decimals (adjust as needed)
    location_data = location_df.select(col('latitude').cast("string").alias('latitude'),
                                      col('longitude').cast("string").alias('longitude'))

  
    json_data = json.dumps(location_data.rdd.map(lambda row: row.asDict()).collect())

    # Write JSON string to file
    with open('/opt/airflow/buffer/location_data.json', 'w') as outfile:
        outfile.write(json_data)

    spark.stop()

