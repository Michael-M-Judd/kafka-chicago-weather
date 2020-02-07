"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_name VARCHAR,
    line VARCHAR,
    station_id INT,
    timestamp DOUBLE
) WITH (
    VALUE_FORMAT = 'avro',
    KEY = 'station_id',
    KAFKA_TOPIC = 'org.chicago.turnstiles.riders.v1'
);

CREATE TABLE turnstile_summary WITH (VALUE_FORMAT='json') as 
  SELECT 
    station_name,
    COUNT(station_id) as count,
    station_id
  FROM turnstile
  GROUP BY station_id, station_name;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    print(resp.content)
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
