# event-intelligence
Cloud-native real-time streaming core built on Apache Kafka and Apache Flink. Designed for scalable event processing with native support for AI models and agents.

## 🐳 Docker Configuration

The `docker-compose.yaml` file orchestrates the main services that are critical for the continuous operation of the whole system:

* **`ZooKeeper`**: Coordinates the Kafka cluster and keeps active sync plus metadata state between different components.
* **`Kafka Broker`**: Streams and buffers live event logs between the data producers side and the consumer applications side.
* **`Kafka UI`**: A web-based administration interface used for browsing topics, partitions, and watching live message flows.
* **`Producer API`**: The FastAPI-powered ingestion doorway that handles incoming streaming requests.
* **`Flink JobManager`**: The master node that governs distributed task coordination, scheduling, and checkpoint boundaries.
* **`Flink TaskManager`**: The actual worker execution container that runs the raw stream memory transformations.
* **`Flink Job Deployer`**: An automation layer that makes sure `flink_job.py` gets deployed right when the cluster boots up.
* **`PostgreSQL Database`**: The centralized operational store for historical warehousing.
* **`Langfuse Server`**: The central monitoring hub for tracking AI agent metrics plus traces.
* **`Langfuse DB (PostgreSQL)`**: A separate database instance dedicated solely to holding Langfuse telemetry and evaluation logs.
