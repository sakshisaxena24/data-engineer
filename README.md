# **Data Pipeline Project**

## **Overview**
This project showcases a complete data pipeline utilizing **Docker**, **PySpark**, **Kafka**, **Cassandra**, and **Airflow**. The pipeline ingests data from Kafka, processes it with PySpark, and stores the processed data in Cassandra. Airflow orchestrates the workflow, and the entire setup is containerized using Docker.

## **Tech Stack**
- **Docker**: Containerization tool for creating isolated environments.
- **Kafka**: Messaging system for data streaming.
- **PySpark**: Python API for Spark to process and analyze streaming data.
- **Cassandra**: NoSQL database for storing processed data.
- **Airflow**: Workflow orchestrator for managing and scheduling tasks.
- **Control Center**: For monitoring and managing Kafka.

## **Setup Instructions**

### **1. Clone the Repository**
```bash
git clone <repository-url>
cd <repository-directory>
```

### **2. Docker Setup**
Ensure you have Docker installed on your machine. Run the following commands to start & stop all the services:
```bash
docker-compose up -d
```
```bash
docker-compose down
```

### **3. Verify Services**
- **Kafka**: Access Kafka on port `9092`.
- **Cassandra**: Access Cassandra on port `9042`.
  - To switch on Cassandra interactive terminal:
    ```bash
    docker exec -it cassandra cqlsh
    ```
  - Describe keyspace and tables:
    ```bash
    DESCRIBE KEYSPACES;
    SELECT * FROM <keyspace>.<table_name>;
    ```
- **Airflow UI**: Access Airflow on `http://localhost:8080`.
- **Spark UI**: Access Spark UI on `http://localhost:9090`.

### **4. Run PySpark Application**
- Ensure necessary jars are in the specified paths.
- Use the following command to submit the Spark job:
  ```bash
  /path/to/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
  --jars /path/to/jars/typesafe-config-1.4.1.jar \
  --master local[2] /path/to/spark_stream.py
  ```

## **Problems Encountered**
- **Dependency Issues**: Missing jars required for Kafka and Cassandra connectors.
- **Configuration Errors**: Incorrect configuration properties for Kafka and Cassandra.
- **Service Connectivity**: Ensuring all Docker containers could communicate properly.

## **Suggested Jars**
- **spark-sql-kafka-0-10_2.12-3.2.1.jar**
- **kafka-clients-2.1.1.jar**
- **spark-streaming-kafka-0-10-assembly_2.12-3.2.1.jar**
- **commons-pool2-2.8.0.jar**
- **spark-token-provider-kafka-0-10_2.12-3.2.1.jar**
- **typesafe-config-1.4.1.jar**

For reference, check out this [project video](https://www.youtube.com/watch?v=GqAcTrqKcrY).
