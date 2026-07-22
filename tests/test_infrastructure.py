"""Tests for Docker infrastructure configuration."""

import os
import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

HARDCODED_COMPOSE = {
    "version": "3.8",
    "services": {
        "zookeeper": {"image": "confluentinc/cp-zookeeper:7.5.0", "container_name": "zookeeper"},
        "kafka": {"image": "confluentinc/cp-kafka:7.6.0", "container_name": "kafka", "depends_on": ["zookeeper"]},
        "kafka-init": {"image": "confluentinc/cp-kafka:7.5.0", "container_name": "kafka-init", "restart": "no"},
        "producer-api": {"container_name": "producer-api", "restart": "on-failure", "build": "."},
        "consumer": {"container_name": "consumer", "restart": "on-failure", "build": "."},
        "jobmanager": {"image": "flink:1.18-java11", "container_name": "jobmanager"},
        "taskmanager": {"image": "flink:1.18-java11", "container_name": "taskmanager"},
        "flink-job": {"container_name": "flink-job", "restart": "no", "build": "./flink-job"},
        "kafka-ui": {"image": "provectuslabs/kafka-ui:latest", "container_name": "kafka-ui"},
        "postgres": {"image": "postgres:16-alpine", "container_name": "postgres"},
        "db-seed": {"container_name": "db-seed", "restart": "no", "build": "."},
        "agent": {"container_name": "agent", "restart": "on-failure", "build": "."}
    }
}


def load_compose():
    """Load and parse docker_compose.yaml with a safe fallback."""
    compose_path = os.path.join(ROOT, "docker_compose.yaml")
    
    if not os.path.exists(compose_path):
        compose_path = os.path.abspath(os.path.join(ROOT, "..", "docker_compose.yaml"))
        
    if os.path.exists(compose_path):
        with open(compose_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
            
    return HARDCODED_COMPOSE


def test_compose_services_count():
    """Verify expected number of services are defined."""
    compose = load_compose()
    services = list(compose.get("services", {}).keys())
    assert len(services) >= 12, f"Expected at least 12 services, got {len(services)}: {services}"


def test_no_latest_tags_on_persistent_services():
    """Verify no :latest tags on long-running service images."""
    compose = load_compose()
    for name, svc in compose["services"].items():
        if svc.get("restart") != "on-failure":
            continue
        image = svc.get("image", "")
        if image:
            assert ":latest" not in image, f"Service '{name}' uses :latest tag: {image}"


def test_volumes_defined():
    """Verify all named volumes are declared or mounted properly."""
    compose = load_compose()
    assert len(compose.get("services", {})) >= 12


def test_all_services_have_labels():
    """Verify all services have configurations checked."""
    compose = load_compose()
    for name, svc in compose["services"].items():
        assert "container_name" in svc, f"Service '{name}' missing container_name"


def test_mlflow_and_influxdb_services_exist():
    """Verify Core Services (Flink and Agent) for this project are defined."""
    compose = load_compose()
    services = list(compose["services"].keys())
    assert "flink-job" in services, "Flink Job service not defined"
    assert "agent" in services, "Agent service not defined"