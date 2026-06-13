# event-intelligence
### This is Malak's branch for testing
Cloud-native real-time streaming core built on Apache Kafka and Apache Flink. Designed for scalable event processing with native support for AI models and agents.

# Notes on `docker-compose.yaml`

This file contains organized notes on the current `docker-compose.yaml` file, based on the current stage of the project and the research direction. The goal is to review the existing services, decide which ones should stay, which ones should be changed, and which ones are no longer needed, then adjust the configuration so it matches the version of the project we want to continue with.

## General idea
The current file is a good starting point because it runs a full local pipeline: data input, Kafka, Flink, database, monitoring/tracing, and a local AI component. However, not everything in it is suitable as-is for the current version of the project, so the task now is not only to run it, but also to review it and adapt it to the project’s actual needs.

## Current services
The services currently included in the file are:

- `zookeeper`
- `kafka`
- `producer-api`
- `jobmanager`
- `taskmanager`
- `flink-job`
- `kafka-ui`
- `postgres`
- `langfuse-db`
- `langfuse`
- `ollama`

## Service analysis

### 1- `zookeeper`
**Current role:** used to support Kafka in the old setup.

**Will it be used?** No, it should be removed.

**Why?**
The project is moving to **Kafka in KRaft mode** instead of ZooKeeper. This means Kafka will manage its own metadata internally without needing a separate ZooKeeper service.

**What should be changed?**
- Remove the `zookeeper` service.
- Remove Kafka’s dependency on ZooKeeper.
- Replace the current Kafka setup with KRaft configuration.

---

### 2- `kafka`
**Current role:** core messaging service in the pipeline.

**Will it be used?** Yes.

**Why?**
Kafka is the backbone of the pipeline. It receives events from the producers and allows Flink and other services to consume the data as streams.

**What should be changed?**
- Move Kafka from ZooKeeper mode to **KRaft**.
- Review `listeners` and `advertised listeners` so they work correctly both inside Docker and from the local machine.
- Decide which topics will be used in the project.
- Decide whether the number of partitions needs to be adjusted.

---

### 3- `producer-api`
**Current role:** a locally built service, most likely responsible for sending events into Kafka.

**Will it be used?** Most likely yes, but its role should be clarified.

**Why?**
The project needs a component that produces data and sends it to Kafka. This service can serve as the main input gateway, or at least as a starting point for the producer layer.

**What should be changed?**
- Clarify the exact role of this service in the pipeline.
- Check what kind of events it sends to Kafka.
- Check whether its current structure matches the actual producer layer planned for the project.
- If needed, update it so it reflects the project’s real data input flow more clearly.
- Move connection-related environment values to `.env` if they are currently hardcoded.

---

### 4- `jobmanager`
**Current role:** the management component of the Flink cluster.

**Will it be used?** Yes.

**Why?**
Flink is the stream processing engine in the project, and `jobmanager` is responsible for coordinating and managing the jobs.

**What should be changed?**
- Make sure the Flink setup used here still matches the way the project currently runs its jobs.
- Make sure the selected Flink version is compatible with the PyFlink code used in the project.
- Move any important configurable values to `.env` later if needed.

---

### 5- `taskmanager`
**Current role:** the worker component that executes Flink tasks.

**Will it be used?** Yes.

**Why?**
`taskmanager` is the part that performs the actual stream processing work. Without it, the Flink jobs cannot run.

**What should be changed?**
- Review the current number of `task slots`.
- Later, memory and parallelism may need adjustment depending on the workload and testing environment.
- Move any important configurable values to `.env` later if needed.

---

### 6- `flink-job`
**Current role:** the container that represents the project’s actual Flink job logic.

**Will it be used?** Yes, and it is one of the most important services.

**Why?**
This is the part that applies the project logic: reading data from Kafka, processing it, cleaning it, calculating metrics or aggregations, and later connecting it with the AI part.

**What should be changed?**
- Make sure the processing logic reflects the current project requirements.
- Add any needed validation or enrichment logic.
- Define more clearly how the AI integration will connect to this part.
- Move any important configurable values to `.env` later if needed.

---

### 7- `kafka-ui`
**Current role:** graphical interface for monitoring Kafka.

**Will it be used?** Yes during development, and it can stay optional later.

**Why?**
It is very useful during testing and debugging because it helps inspect topics, messages, consumers, and general Kafka activity.

**What should be changed?**
- No major change is needed now.
- Later, decide whether it should remain in the final stack or be considered a development-only tool.

---

### 8- `postgres`
**Current role:** the main relational database for the project.

**Will it be used?** Yes.

**Why?**
The project needs a database to store outputs or processed results, and PostgreSQL fits this role well.

**What should be changed?**
- Review the database name and whether it still matches the current project structure.
- Decide whether a persistent volume should be added so the database data remains available after containers are stopped and started again.
- Move database credentials to `.env` instead of keeping them directly inside the compose file.

---

### 9- `langfuse-db`
**Current role:** a dedicated database for Langfuse.

**Will it be used?** Yes, if Langfuse stays in the project.

**Why?**
Langfuse needs its own database to store its internal data, and keeping it separate from the main project database is a good setup.

**What should be changed?**
- Review whether Langfuse is part of the intended final setup.
- Move its database credentials to `.env` if they remain hardcoded.

---

### 10- `langfuse`
**Current role:** monitoring and tracing service.

**Will it be used?** Most likely yes, but its exact role should be decided.

**Why?**
Since the pipeline includes an AI-related part, Langfuse can be useful for tracing AI calls, monitoring outputs, and understanding what happens during experiments.

**What should be changed?**
- Decide whether it will be part of the final architecture or mainly used during development.
- Clarify how it will connect to the AI layer.
- Move its secrets to `.env` instead of keeping them inside the compose file.

---

### 11- `ollama`
**Current role:** local service for running an AI model.

**Will it be used?** Yes, and it is an important part of the next stage.

**Why?**
It provides a practical way to run a local model and connect it to the pipeline.

**What should be changed?**
- Decide which model will be used.
- Decide which component will talk to it: `flink-job`, `producer-api`, or a separate service.
- Decide whether inference should happen for every event or only for selected cases.
- Move any model-related or connection-related settings to `.env` later if needed.

## Quick summary: what stays and what changes?

### To remove
- `zookeeper`

### To keep with changes
- `kafka`
- `producer-api`
- `flink-job`
- `postgres`
- `langfuse`
- `langfuse-db`
- `ollama`

### To keep as they are for now
- `jobmanager`
- `taskmanager`
- `kafka-ui`

## Main changes needed in the file

1. Remove `zookeeper` completely.
2. Reconfigure Kafka to use **KRaft mode**.
3. Review `producer-api` so its role and configuration match the project’s actual data input flow.
4. Review `flink-job` because it carries the main processing logic and the future AI integration.
5. Review the PostgreSQL and Langfuse configuration and clean up unnecessary hardcoded values.
6. Define the role of `ollama` more clearly inside the architecture.
7. Later, it would be useful to add `healthchecks` for the main services.
8. As a final cleanup step, move sensitive values and configurable settings to `.env` where appropriate.

## Final note
The current `docker-compose.yaml` is a strong starting point for local setup and testing, but it still needs adjustment so it better matches the version of the project we want to continue with. The most important change for now is removing `zookeeper` and moving to **KRaft**, then reviewing the remaining services so they support ingestion, processing, storage, and AI integration in a cleaner and more focused way.
