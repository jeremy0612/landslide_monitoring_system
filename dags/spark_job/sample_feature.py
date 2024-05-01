from pyspark.sql import SparkSession
from pyspark.sql.functions import col,  when, lit

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
    
    weather_data_df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://postgres:5432/landslide_warehouse")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "weather_data")\
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
    
    datetime_df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://postgres:5432/landslide_warehouse")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "datetime")\
        .option("user", "admin")\
        .option("password", "123456")\
        .load()
    
    soil_df = soil_df.withColumn("soil_temp_0_to_7", 
                             when(col("depth") == lit("0 to 7 cm"), col("temperature").cast("float"))
                             .otherwise(lit(0)))\
                    .withColumn("soil_temp_7_to_28",
                             when(col("depth") == lit("7 to 28 cm"), col("temperature").cast("float"))
                             .otherwise(lit(0)))\
                    .withColumn("soil_temp_28_to_100",
                             when(col("depth") == lit("28 to 100 cm"), col("temperature").cast("float"))
                             .otherwise(lit(0)))\
                    .withColumn("soil_temp_100_255",
                             when(col("depth") == lit("100 to 255 cm"), col("temperature").cast("float"))
                             .otherwise(lit(0)))

    soil_df = soil_df.withColumn("soil_moisture_0_to_7",
                             when(col("depth") == lit("0 to 7 cm"), col("moisture").cast("float"))
                             .otherwise(lit(0)))\
                    .withColumn("soil_moisture_7_to_28",
                             when(col("depth") == lit("7 to 28 cm"), col("moisture").cast("float"))
                             .otherwise(lit(0)))\
                    .withColumn("soil_moisture_28_to_100",
                             when(col("depth") == lit("28 to 100 cm"), col("moisture").cast("float"))
                             .otherwise(lit(0)))\
                    .withColumn("soil_moisture_100_255",
                             when(col("depth") == lit("100 to 255 cm"), col("moisture").cast("float"))
                             .otherwise(lit(0)))    
    soil_df = soil_df.select(col("weather_data_id"),\
                            col("soil_temp_0_to_7"), col("soil_temp_7_to_28"), col("soil_temp_28_to_100"), col("soil_temp_100_255"),\
                            col("soil_moisture_0_to_7"), col("soil_moisture_7_to_28"), col("soil_moisture_28_to_100"),col("soil_moisture_100_255"))
    soil_df = soil_df.groupby('weather_data_id')\
                    .agg({'soil_temp_0_to_7': 'max', 'soil_temp_7_to_28': 'max', 'soil_temp_28_to_100': 'max', 'soil_temp_100_255': 'max',\
                          'soil_moisture_0_to_7': 'max', 'soil_moisture_7_to_28': 'max', 'soil_moisture_28_to_100': 'max', 'soil_moisture_100_255': 'max'})\
                    .orderBy('weather_data_id')
    soil_df = soil_df.withColumnRenamed('max(soil_temp_0_to_7)', 'soil_temp_0_to_7')\
                    .withColumnRenamed('max(soil_temp_7_to_28)', 'soil_temp_7_to_28')\
                    .withColumnRenamed('max(soil_temp_28_to_100)', 'soil_temp_28_to_100')\
                    .withColumnRenamed('max(soil_temp_100_255)', 'soil_temp_100_255')\
                    .withColumnRenamed('max(soil_moisture_0_to_7)', 'soil_moisture_0_to_7')\
                    .withColumnRenamed('max(soil_moisture_7_to_28)', 'soil_moisture_7_to_28')\
                    .withColumnRenamed('max(soil_moisture_28_to_100)', 'soil_moisture_28_to_100')\
                    .withColumnRenamed('max(soil_moisture_100_255)', 'soil_moisture_100_255')
    
    data_df = weather_data_df.join(soil_df, \
                                    on=[soil_df['weather_data_id']==weather_data_df['fact_id']],\
                                    how='inner')\
                                    .select('fact_id','location_id','datetime_id','temperature','precipitation','rain','relative_humidity',\
                                            'soil_temp_0_to_7','soil_temp_7_to_28','soil_temp_28_to_100','soil_temp_100_255',\
                                            'soil_moisture_0_to_7','soil_moisture_7_to_28','soil_moisture_28_to_100','soil_moisture_100_255')
    
    data_df = data_df.join(datetime_df, on='datetime_id', how='inner')
    event_df = landslide_event_df.join(datetime_df, on='datetime_id', how='inner')\
                                .select(col('event_id'), col('landslide_type'), \
                                         col('size'), col('location_id'),col('date'),col('month'),col('year'))

    merged_df = event_df.join(data_df, 
                            on=[event_df['location_id'] == data_df['location_id'],\
                                event_df['date'] == data_df['date'],\
                                event_df['month'] == data_df['month'],\
                                event_df['year'] == data_df['year']], \
                            how='inner')\
                        .sort('event_id','time')
    
    merged_df = merged_df.select('event_id', 'landslide_type', 'size',\
                                 'time', event_df['location_id'],\
                                 'temperature', 'rain', 'precipitation', 'relative_humidity',\
                                 'soil_temp_0_to_7','soil_temp_7_to_28','soil_temp_28_to_100','soil_temp_100_255',\
                                 'soil_moisture_0_to_7','soil_moisture_7_to_28','soil_moisture_28_to_100','soil_moisture_100_255')
    merged_df = merged_df.dropDuplicates(subset=['location_id', 'time'])

    # processed_df.show()
    merged_df.show(48)
    merged_df.printSchema()

    spark.stop()

