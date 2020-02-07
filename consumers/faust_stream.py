"""Defines trends calculations for stations"""
import logging

import faust
from models import Line


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


    # Didn't realize we wanted it named this... Others are named differently
TABLE_TOPIC = "org.chicago.cta.stations.table.v1"
app = faust.App("stations-stream",
                broker="kafka://localhost:9092", store="memory://")

stations_topic = app.topic(
    "org.chicago.stations", value_type=Station)

transformed_stations_topic = app.topic(
    TABLE_TOPIC, value_type=TransformedStation, partitions=1)

table = app.Table(
    TABLE_TOPIC,
    default=TransformedStation,
    partitions=1,
    changelog_topic=transformed_stations_topic,
)


def get_line(station):
    if station.red:
        return 'red'
    elif station.blue:
        return 'blue'
    elif station.green:
        return 'green'
    else:
        logger.warn(f"Unknown station color for station {station.station_id}")
        return "unknown"


@app.agent(stations_topic)
async def process_stations(stations):
    """
    Process a stream of station data (Station) and produces
    transformed station data (TransformedStation)
    """
    async for station in stations:

        color = get_line(station)

        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=color
        )

        table[station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
