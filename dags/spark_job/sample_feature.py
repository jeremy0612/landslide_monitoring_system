from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, explode, split
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


    # Load weather and soil data together (inner join)
    data2_df = weather_data_df.join(soil_df, \
                                    on=[soil_df['weather_data_id']==weather_data_df['fact_id']],\
                                    how='inner')\
                                .withColumn('temp_range', explode('depth'))\
                                .withColumn('soil_temp', soil_df['temperature'])\
                                .withColumn('soil_moisture', soil_df['moisture'])\
                                .sort('fact_id','depth')
    # Split depth into an array (assuming the depth is separated by spaces)
    data2_df = data2_df.withColumn("depth_array", split(col("depth"), " "))

    # Split and process depth data using explode and regexp_extract (option 2)
    processed_df = data2_df.withColumn(
        "depth_exploded", explode(array([col("depth_array")]))
    )
    # Extract temperature, moisture, depth_start, and depth_end
    processed_df = processed_df.withColumn("depth_start", col("temp_range").getItem(0)) \
                                .withColumn("depth_end", col("temp_range").getItem(2))
    processed_df.show()

    data_df = weather_data_df.join(datetime_df, on='datetime_id', how='inner')\
                            .select('location_id', 'date','month','year', 'temperature', 'rain',\
                                    'precipitation','relative_humidity', 'time')    
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
                                 'temperature', 'rain', 'precipitation', 'relative_humidity')
    merged_df = merged_df.dropDuplicates(subset=['location_id', 'time'])

    merged_df.show(48)
    merged_df.printSchema()

    spark.stop()

