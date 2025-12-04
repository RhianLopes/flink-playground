import os
import json
from typing import Any, Dict, List, Optional

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import RowKind
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema

from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaRecordSerializationSchema,
)

from pyflink.table import (
    EnvironmentSettings,
    StreamTableEnvironment,
)

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "kafka.kafka.svc.cluster.local:9092",
)


def row_list_to_dicts(
    rows: Optional[List[Any]],
    fields: List[str],
) -> Optional[List[Dict[str, Any]]]:
    """
    Converte uma lista de Row (ARRAY<ROW(...)> vindo da Table API)
    em lista de dict para poder serializar em JSON.
    """
    if rows is None:
        return None
    out: List[Dict[str, Any]] = []
    for r in rows:
        if r is None:
            continue
        d = {field: r[i] for i, field in enumerate(fields)}
        out.append(d)
    return out if out else None

def create_tables_and_views(t_env: StreamTableEnvironment) -> None:
    t_env.execute_sql(f"""
        CREATE TABLE freight_events (
          freight_id  BIGINT,
          state       STRING,
          `timestamp` TIMESTAMP_LTZ(3),

          -- headers do Kafka como metadata
          headers MAP<STRING, BYTES> METADATA FROM 'headers',

          WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
          'connector' = 'kafka',
          'topic' = 'freight.events',
          'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
          'properties.security.protocol' = 'PLAINTEXT',
          'properties.group.id' = 'freight-events-consumer',
          'scan.startup.mode' = 'earliest-offset',
          'format' = 'json',
          'json.ignore-parse-errors' = 'true',
          'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    t_env.execute_sql("""
        CREATE VIEW freight_events_enriched AS
        SELECT
          freight_id,
          state,
          `timestamp`,
          CAST(headers['EventType'] AS STRING) AS event_type
        FROM freight_events
    """)

    t_env.execute_sql(f"""
        CREATE TABLE tracking_events (
          event_type    STRING,
          click_id      STRING,
          freight_id    BIGINT,
          trucker_uuid  STRING,
          `timestamp`   TIMESTAMP_LTZ(3),
          WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
          'connector' = 'kafka',
          'topic' = 'tracking.events',
          'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
          'properties.security.protocol' = 'PLAINTEXT',
          'properties.group.id' = 'tracking-events-consumer',
          'scan.startup.mode' = 'earliest-offset',
          'format' = 'json',
          'json.ignore-parse-errors' = 'true',
          'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    t_env.execute_sql("""
        CREATE VIEW freight_history AS
        SELECT
          freight_id,
          ARRAY_AGG(
            ROW(
              event_type,
              state,
              CAST(`timestamp` AS STRING)
            )
          ) AS freight_history
        FROM freight_events_enriched
        GROUP BY freight_id
    """)

    t_env.execute_sql("""
        CREATE VIEW freight_click_history AS
        SELECT
          freight_id,
          ARRAY_AGG(
            ROW(
              event_type,
              click_id,
              trucker_uuid,
              CAST(`timestamp` AS STRING)
            )
          ) AS click_history
        FROM tracking_events
        GROUP BY freight_id
    """)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    create_tables_and_views(t_env)

    leads_table = t_env.sql_query("""
        SELECT
          h.freight_id,
          h.freight_history,
          c.click_history
        FROM freight_history h
        JOIN freight_click_history c
          ON h.freight_id = c.freight_id
        -- opcional:
        -- WHERE CARDINALITY(c.click_history) > 0
    """)

    leads_changelog = t_env.to_changelog_stream(leads_table)

    def process_row(row) -> Optional[str]:
        if row.get_row_kind() not in (RowKind.INSERT, RowKind.UPDATE_AFTER):
            return None

        freight_id = row[0]
        freight_history_raw = row[1]
        click_history_raw = row[2]

        freight_history = row_list_to_dicts(
            freight_history_raw,
            ["event_type", "state", "ts"],
        )
        click_history = row_list_to_dicts(
            click_history_raw,
            ["event_type", "click_id", "trucker_uuid", "ts"],
        )

        payload = {
            "freight_id": freight_id,
            "freight_history": freight_history,
            "click_history": click_history,
        }

        return json.dumps(payload, ensure_ascii=False)

    events_stream = (
        leads_changelog
        .map(process_row, output_type=Types.STRING())
        .filter(lambda v: v is not None)
    )

    record_serializer = (
        KafkaRecordSerializationSchema
        .builder()
        .set_topic("freight-leads.events")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    kafka_sink = (
        KafkaSink
        .builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(record_serializer)
        .build()
    )

    events_stream.sink_to(kafka_sink)

    env.execute("freight-leads-events")

if __name__ == "__main__":
    main()
