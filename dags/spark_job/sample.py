from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.functions import collect_list, struct, explode, array
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
    
    data = [
        (1, 1.0, 2.0, 3.0),
        (1, 4.0, 5.0, 6.0),
        (2, 7.0, 8.0, 9.0),
        (2, 10.0, 11.0, 12.0),
    ]

    # Create a DataFrame
    df = spark.createDataFrame(data, ["id", "a", "b","c"])

    # Group by 'id' and collect features as an array of vectors
    grouped_df = df.groupBy("id") \
                .agg(collect_list('a') \
                        .alias("features_array_a"),\
                            collect_list('b') \
                        .alias("features_array_b"),\
                            collect_list('c') \
                        .alias("features_array_c"))

    # Show the results
    grouped_df.show()


    # soil_df = soil_df.withColumn("soil_temp_0_to_7", 
    #                          when(col("depth") == lit("0 to 7 cm"), col("temperature").cast("float"))
    #                          .otherwise(lit(0)))\
    #                 .withColumn("soil_temp_7_to_28",
    #                          when(col("depth") == lit("7 to 28 cm"), col("temperature").cast("float"))
    #                          .otherwise(lit(0)))\
    #                 .withColumn("soil_temp_28_to_100",
    #                          when(col("depth") == lit("28 to 100 cm"), col("temperature").cast("float"))
    #                          .otherwise(lit(0)))\
    #                 .withColumn("soil_temp_100_255",
    #                          when(col("depth") == lit("100 to 255 cm"), col("temperature").cast("float"))
    #                          .otherwise(lit(0)))

    # soil_df = soil_df.withColumn("soil_moisture_0_to_7",
    #                          when(col("depth") == lit("0 to 7 cm"), col("moisture").cast("float"))
    #                          .otherwise(lit(0)))\
    #                 .withColumn("soil_moisture_7_to_28",
    #                          when(col("depth") == lit("7 to 28 cm"), col("moisture").cast("float"))
    #                          .otherwise(lit(0)))\
    #                 .withColumn("soil_moisture_28_to_100",
    #                          when(col("depth") == lit("28 to 100 cm"), col("moisture").cast("float"))
    #                          .otherwise(lit(0)))\
    #                 .withColumn("soil_moisture_100_255",
    #                          when(col("depth") == lit("100 to 255 cm"), col("moisture").cast("float"))
    #                          .otherwise(lit(0)))    
    # soil_df = soil_df.select(col("weather_data_id"),\
    #                         col("soil_temp_0_to_7"), col("soil_temp_7_to_28"), col("soil_temp_28_to_100"), col("soil_temp_100_255"),\
    #                         col("soil_moisture_0_to_7"), col("soil_moisture_7_to_28"), col("soil_moisture_28_to_100"),col("soil_moisture_100_255"))
    # soil_df = soil_df.groupby('weather_data_id')\
    #                 .agg({'soil_temp_0_to_7': 'max', 'soil_temp_7_to_28': 'max', 'soil_temp_28_to_100': 'max', 'soil_temp_100_255': 'max',\
    #                       'soil_moisture_0_to_7': 'max', 'soil_moisture_7_to_28': 'max', 'soil_moisture_28_to_100': 'max', 'soil_moisture_100_255': 'max'})\
    #                 .orderBy('weather_data_id')
    # soil_df = soil_df.withColumnRenamed('max(soil_temp_0_to_7)', 'soil_temp_0_to_7')\
    #                 .withColumnRenamed('max(soil_temp_7_to_28)', 'soil_temp_7_to_28')\
    #                 .withColumnRenamed('max(soil_temp_28_to_100)', 'soil_temp_28_to_100')\
    #                 .withColumnRenamed('max(soil_temp_100_255)', 'soil_temp_100_255')\
    #                 .withColumnRenamed('max(soil_moisture_0_to_7)', 'soil_moisture_0_to_7')\
    #                 .withColumnRenamed('max(soil_moisture_7_to_28)', 'soil_moisture_7_to_28')\
    #                 .withColumnRenamed('max(soil_moisture_28_to_100)', 'soil_moisture_28_to_100')\
    #                 .withColumnRenamed('max(soil_moisture_100_255)', 'soil_moisture_100_255')
    
    # data_df = weather_data_df.join(soil_df, \
    #                                 on=[soil_df['weather_data_id']==weather_data_df['fact_id']],\
    #                                 how='inner')\
    #                                 .select('fact_id','location_id','datetime_id','temperature','precipitation','rain','relative_humidity',\
    #                                         'soil_temp_0_to_7','soil_temp_7_to_28','soil_temp_28_to_100','soil_temp_100_255',\
    #                                         'soil_moisture_0_to_7','soil_moisture_7_to_28','soil_moisture_28_to_100','soil_moisture_100_255')
    
    # data_df.show()



    # df = location_df.join(landslide_event_df, on='location_id', how='inner')
    
    # # df.printSchema()

    # # Select relevant columns (latitude, longitude)
    # # Define a lower precision for decimals (adjust as needed)
    # location_data = location_df.select(col('latitude').cast("string").alias('latitude'),
    #                                   col('longitude').cast("string").alias('longitude'))

  
    # json_data = json.dumps(location_data.rdd.map(lambda row: row.asDict()).collect())

    # # Write JSON string to file
    # with open('/opt/airflow/buffer/location_data.json', 'w') as outfile:
    #     outfile.write(json_data)

    spark.stop()

