import json
import logging
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

from confluent_kafka import Producer, KafkaError


@dataclass
class AppConfig:
    kafka_bootstrap: str
    topic_freight_events: str
    topic_tracking_events: str
    event_interval_seconds: float
    client_id: str


def load_config_from_env() -> AppConfig:
    return AppConfig(
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka.kafka.svc.cluster.local:9092"),
        topic_freight_events=os.getenv("TOPIC_FREIGHT_EVENTS", "freight.events"),
        topic_tracking_events=os.getenv("TOPIC_TRACKING_EVENTS", "tracking.events"),
        event_interval_seconds=float(os.getenv("EVENT_INTERVAL_SECONDS", "1")),
        client_id=os.getenv("CLIENT_ID", "freight-events-simulator"),
    )


def setup_logging() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


logger = logging.getLogger("freight_simulator")


def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


@dataclass
class SimulatedState:
    freight_states: Dict[int, str]
    next_freight_id: int


def build_initial_state() -> SimulatedState:
    return SimulatedState(
        freight_states={},
        next_freight_id=1,
    )


def build_kafka_producer(config: AppConfig) -> Producer:
    conf = {
        "bootstrap.servers": config.kafka_bootstrap,
        "client.id": config.client_id,
        "enable.idempotence": True,
        "retries": 3,
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "snappy",
    }
    logger.info("Criando Kafka Producer com bootstrap=%s", config.kafka_bootstrap)
    return Producer(conf)


def delivery_report(err: Optional[KafkaError], msg) -> None:
    if err is not None:
        logger.error(
            "Falha ao entregar mensagem: topic=%s partition=%s error=%s",
            msg.topic(),
            msg.partition(),
            err,
        )
    else:
        logger.debug(
            "Mensagem entregue: topic=%s partition=%s offset=%s key=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.key().decode("utf-8") if msg.key() else None,
        )


def create_freight_event(
    producer: Producer,
    app_config: AppConfig,
    state: SimulatedState,
) -> None:

    existing_non_deleted = [
        f for f, s in state.freight_states.items() if s != "deleted"
    ]

    if not existing_non_deleted or random.random() < 0.4:
        freight_id = state.next_freight_id
        state.next_freight_id += 1
        state.freight_states[freight_id] = "active"
        event_type = "freight.created"
        new_state = "active"
    else:
        freight_id = random.choice(existing_non_deleted)
        current_state = state.freight_states[freight_id]

        if current_state == "active":
            transition = random.choice(["deactivate", "delete"])
        elif current_state == "inactive":
            transition = random.choice(["activate", "delete"])
        else:
            return

        if transition == "deactivate":
            state.freight_states[freight_id] = "inactive"
            event_type = "freight.deactivated"
            new_state = "inactive"
        elif transition == "activate":
            state.freight_states[freight_id] = "active"
            event_type = "freight.activated"
            new_state = "active"
        else:
            state.freight_states[freight_id] = "deleted"
            event_type = "freight.deleted"
            new_state = "deleted"

    ts = now_iso()

    key = str(freight_id).encode("utf-8")
    value = {
        "freight_id": freight_id,
        "state": new_state,
        "timestamp": ts,
    }
    headers = [
        ("EventType", event_type),
        ("EventTimestamp", ts),
        ("SourceApp", "freight-events-simulator"),
        ("EntityType", "freight"),
    ]

    producer.produce(
        topic=app_config.topic_freight_events,
        key=key,
        value=json.dumps(value).encode("utf-8"),
        headers=headers,
        callback=delivery_report,
    )

    logger.info("[FREIGHT] %s (event_type=%s via header)", value, event_type)


def create_click_event(
    producer: Producer,
    app_config: AppConfig,
    state: SimulatedState,
) -> bool:

    active_freights = [
        f for f, s in state.freight_states.items() if s == "active"
    ]
    if not active_freights:
        return False

    freight_id = random.choice(active_freights)
    trucker_uuid = str(uuid.uuid4())
    click_id = str(uuid.uuid4())
    ts = now_iso()

    key = str(freight_id).encode("utf-8")
    value = {
        "event_type": "freight.click",
        "click_id": click_id,
        "freight_id": freight_id,
        "trucker_uuid": trucker_uuid,
        "timestamp": ts,
    }
    headers = [
        ("EventType", "freight.click"),
        ("EventTimestamp", ts),
        ("SourceApp", "freight-events-simulator"),
        ("EntityType", "tracking"),
    ]

    producer.produce(
        topic=app_config.topic_tracking_events,
        key=key,
        value=json.dumps(value).encode("utf-8"),
        headers=headers,
        callback=delivery_report,
    )

    logger.info("[CLICK]   %s", value)
    return True


shutdown_flag = False


def _handle_signal(signum, frame) -> None:
    global shutdown_flag
    logger.warning("Sinal recebido (%s). Encerrando com segurança...", signum)
    shutdown_flag = True


def main() -> None:
    setup_logging()
    config = load_config_from_env()
    state = build_initial_state()
    producer = build_kafka_producer(config)

    logger.info("Iniciando gerador de eventos Kafka...")
    logger.info("- bootstrap: %s", config.kafka_bootstrap)
    logger.info("- tópicos: %s, %s", config.topic_freight_events, config.topic_tracking_events)
    logger.info("- intervalo (s): %s", config.event_interval_seconds)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        while not shutdown_flag:
            if random.random() < 0.5:
                create_freight_event(producer, config, state)
            else:
                if not create_click_event(producer, config, state):
                    create_freight_event(producer, config, state)

            producer.poll(0)
            time.sleep(config.event_interval_seconds)

    finally:
        producer.flush()
        logger.info("Encerrado.")


if __name__ == "__main__":
    main()
