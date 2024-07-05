from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Simple ETL Job") \
        .getOrCreate()

    # Read data
    df = spark.read.csv('/home/nikhil/simple-etl-pipline/data/employee_data.csv', header=True, inferSchema=True)

    # Perform transformations
    df_transformed = df.filter(df['salary'] > 40000.00)

    # Write data to output
    df_transformed.write.csv('/home/nikhil/simple-etl-pipline/data/output/', header=True)

    spark.stop()

if __name__ == "__main__":
    main()