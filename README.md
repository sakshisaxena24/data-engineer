
### README

#### Project Overview
This project demonstrates a complete data pipeline using Docker, PySpark, Kafka, Cassandra, and Airflow. The pipeline ingests data from Kafka, processes it using PySpark, and stores the processed data in Cassandra. Airflow is used to orchestrate the workflow. The entire setup is containerized using Docker.

#### Tech Stack
- **Docker**: Containerization tool to create isolated environments for the services.
- **Kafka**: Messaging system to stream data.
- **PySpark**: Python API for Spark to process and analyze the streaming data.
- **Cassandra**: NoSQL database for storing processed data.
- **Airflow**: Workflow orchestrator for managing and scheduling the data pipeline tasks.
- **Control Center**: For monitoring and managing Kafka.

#### Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Docker Setup**
   Ensure you have Docker installed on your machine. Run the following command to start & stop all the services:
   ```bash
   docker-compose up -d
   ```

    ```bash
   docker-compose down
   ```

3. **Verify Services**
   - **Kafka**: Access Kafka on port 9092.
   - **Cassandra**: Access Cassandra on port 9042.
To switch on cassandra interactive terminal:
   ```bash
   docker exec -it cassandra cqlsh

   ```
Describe keyspace and tables:
```bash
   DESCRIBE KEYSPACES;
   Select * from <keyspace>.<table_name>

   ```
   - **Airflow UI**: Access Airflow on `http://localhost:8080`.
   - **Spark UI**: Access Spark UI on `http://localhost:9090`.

4. **Run PySpark Application**
   - Make sure to have the necessary jars in the specified paths.
   - Use the following command to submit the Spark job:
     ```bash
     /path/to/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
     --jars /path/to/jars/typesafe-config-1.4.1.jar \
     --master local[2] /path/to/spark_stream.py
     ```

#### Problems Encountered
- **Dependency Issues**: Missing jars required for Kafka and Cassandra connectors.
- **Configuration Errors**: Incorrect configuration properties for Kafka and Cassandra.
- **Service Connectivity**: Ensuring all Docker containers could communicate properly.

#### Suggested Jars
- **spark-sql-kafka-0-10_2.12-3.2.1.jar**
- **kafka-clients-2.1.1.jar**
- **spark-streaming-kafka-0-10-assembly_2.12-3.2.1.jar**
- **commons-pool2-2.8.0.jar**
- **spark-token-provider-kafka-0-10_2.12-3.2.1.jar**
- **typesafe-config-1.4.1.jar**

Link to reference project: [https://www.youtube.com/watch?v=GqAcTrqKcrY](https://youtu.be/GqAcTrqKcrY?si=2tiKwTyg_jchR55a)

