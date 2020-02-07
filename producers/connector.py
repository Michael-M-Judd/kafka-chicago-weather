"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"
# this should not be plain text in a real app :)
DATABASE_URL = "jdbc:postgresql://localhost:5432/cta"
DATABASE_USERNAME = "cta_admin"
DATABASE_PW = "chicago"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    try:
        resp = requests.post(
            KAFKA_CONNECT_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps({
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "batch.max.rows": "500",
                    "connection.url": DATABASE_URL,
                    "connection.user": DATABASE_USERNAME,
                    "connection.password": DATABASE_PW,
                    "table.whitelist": "stations",
                    "mode": "incrementing",
                    "incrementing.column.name": "stop_id",
                    "topic.prefix": "org.chicago.",
                    "poll.interval.ms": "1800000",
                }
            }),
        )

        resp.raise_for_status()
        logging.debug("connector created successfully")

    except Exception as e:
        logging.error(f"Error creating JDBC connector {e}")


if __name__ == "__main__":
    configure_connector()
