FROM flink:1.18-java11

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.18.1

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

WORKDIR /opt/flink/jobs
COPY flink_job.py .

CMD ["/bin/bash", "-c", "sleep 60 && /opt/flink/bin/flink run -d -m jobmanager:8081 -py /opt/flink/jobs/flink_job.py"]