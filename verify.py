"""Read-only verification after producers and the filter have stopped."""
import json
from collections import Counter

from kafka import KafkaConsumer

from configs import BOOTSTRAP_SERVERS, HUMIDITY_TOPIC, SENSORS_TOPIC, TEMPERATURE_TOPIC
from filter_sensors import get_alerts


def read_topic(topic):
    consumer = KafkaConsumer(
        topic, bootstrap_servers=BOOTSTRAP_SERVERS, group_id=None,
        auto_offset_reset="earliest", enable_auto_commit=False,
        consumer_timeout_ms=3000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    try:
        return [message.value for message in consumer]
    finally:
        consumer.close()


def main():
    boundary_cases = [
        (40, 20, []), (40, 80, []), (40.01, 50, [TEMPERATURE_TOPIC]),
        (35, 19.99, [HUMIDITY_TOPIC]), (35, 80.01, [HUMIDITY_TOPIC]),
        (41, 19, [TEMPERATURE_TOPIC, HUMIDITY_TOPIC]),
        (25, 15, [HUMIDITY_TOPIC]), (45, 85, [TEMPERATURE_TOPIC, HUMIDITY_TOPIC]),
    ]
    for temperature, humidity, expected in boundary_cases:
        data = {"id": "boundary-test", "timestamp": 0,
                "temperature": temperature, "humidity": humidity}
        assert [topic for topic, _ in get_alerts(data)] == expected
    source = read_topic(SENSORS_TOPIC)
    temperatures = read_topic(TEMPERATURE_TOPIC)
    humidities = read_topic(HUMIDITY_TOPIC)
    assert source, "No input records"
    assert temperatures and humidities, "Both alert types must be demonstrated"
    sensor_counts = Counter(data["id"] for data in source)
    assert len(sensor_counts) >= 2
    expected = []
    for data in source:
        assert 25 <= data["temperature"] <= 45
        assert 15 <= data["humidity"] <= 85
        assert data["timestamp"] > 0
        expected.extend(get_alerts(data))
    actual = [(TEMPERATURE_TOPIC, data) for data in temperatures]
    actual += [(HUMIDITY_TOPIC, data) for data in humidities]
    canonical = lambda pairs: Counter((topic, json.dumps(data, sort_keys=True)) for topic, data in pairs)
    assert canonical(actual) == canonical(expected), "Output differs from exact threshold rules"
    print(json.dumps({"result": "PASS", "source_records": len(source),
                      "sensor_counts": dict(sensor_counts),
                      "temperature_alerts": len(temperatures), "humidity_alerts": len(humidities),
                      "boundary_cases": len(boundary_cases),
                      "output_matches_every_input": True}, indent=2))


if __name__ == "__main__":
    main()
