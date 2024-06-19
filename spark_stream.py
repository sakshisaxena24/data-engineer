import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

jars = ",".join([
    "/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.2.1.jar",
    "/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/kafka-clients-2.1.1.jar",
    "/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/spark-streaming-kafka-0-10-assembly_2.12-3.2.1.jar",
    "/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/commons-pool2-2.8.0.jar",
    "/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.2.1.jar",
    "/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/scala-library-2.12.10.jar"
])
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streaming
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")

def create_table(session):
    session.set_keyspace('spark_streaming')
    session.execute("""
    CREATE TABLE IF NOT EXISTS created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    logging.info("Table created successfully!")

def create_spark_connection():
    try:
        spark = (SparkSession.builder
                 .appName("KafkaCassandraIntegration")
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1")
                 .config("spark.jars",
                         "/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.2.1.jar,/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/kafka-clients-2.1.1.jar,/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/spark-streaming-kafka-0-10-assembly_2.12-3.2.1.jar,/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/commons-pool2-2.8.0.jar,/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.2.1.jar,/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/scala-library-2.12.10.jar,/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/spark-cassandra-connector-assembly_2.12-3.2.0.jar,/Users/sakshisaxena/PycharmProjects/data-engineering/venv/lib/python3.11/site-packages/pyspark/jars/typesafe-config-1.4.1.jar")
                 .config("spark.cassandra.connection.host", "localhost")
                 .getOrCreate())

        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
        return None

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
        return None

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        logging.info("Cassandra connection created successfully!")
        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    sel_df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    logging.info("Selection dataframe created from Kafka")
    return sel_df

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()
            if session is not None:
                create_keyspace(session)
                create_table(session)

                #Test Spark to Cassandra connection
                # Test Spark to Cassandra connection
                # df = (spark_conn.read
                #       .format("org.apache.spark.sql.cassandra")
                #       .options(table="your_table", keyspace="your_keyspace")
                #       .load())

                # df.show()

                logging.info("Streaming is being started...")
                streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .option('keyspace', 'spark_streaming')
                                   .option('table', 'created_users')
                                   .start())
                streaming_query.awaitTermination()
