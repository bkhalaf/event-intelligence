from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import time
import sys

def create_kafka_source_ddl():
    return """
    CREATE TABLE kafka_source (
        data ROW<message STRING>,
        `timestamp` DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'test-topic',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'flink-consumer-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """

def create_kafka_sink_ddl():
    return """
    CREATE TABLE kafka_sink (
        `original_message` STRING,
        `transformed_message` STRING,
        `processed_at` TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'output-topic',
        'properties.bootstrap.servers' = 'kafka:29092',
        'format' = 'json'
    )
    """

def create_transformation_query():
    return """
    INSERT INTO kafka_sink
    SELECT
        data.`message` AS original_message,
        UPPER(data.`message`) AS transformed_message,
        CURRENT_TIMESTAMP AS processed_at
    FROM kafka_source
    WHERE data.`message` IS NOT NULL
    """

def main():
    #print("Waiting for Kafka to be ready...")
    #time.sleep(90)

    print("Initializing Flink environment...")
    sys.stdout.flush()

    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)

        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = StreamTableEnvironment.create(env, environment_settings=settings)

        kafka_jar = "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
        t_env.get_config().set("pipeline.jars", kafka_jar)

        print("Creating source table (test-topic)...")
        sys.stdout.flush()
        t_env.execute_sql(create_kafka_source_ddl())

        print("Creating sink table (output-topic)...")
        sys.stdout.flush()
        t_env.execute_sql(create_kafka_sink_ddl())

        print("Starting Flink job: Reading order messages from 'test-topic', transforming to UPPERCASE, writing to 'output-topic'")
        print("Job is running... waiting for messages")
        sys.stdout.flush()

        table_result = t_env.execute_sql(create_transformation_query())
        table_result.wait()

    except Exception as e:
        print(f"ERROR: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()