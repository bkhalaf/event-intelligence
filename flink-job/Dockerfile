FROM flink:1.18-java11

# Install Python and PyFlink
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install apache-flink==1.18.1 && \
    apt-get clean

# Download Kafka connector
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

WORKDIR /opt/flink/jobs

COPY flink_job.py .

CMD ["python3", "flink_job.py"]
