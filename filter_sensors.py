import argparse
import json

from kafka import KafkaConsumer, KafkaProducer

from configs import BOOTSTRAP_SERVERS, HUMIDITY_TOPIC, PREFIX, SENSORS_TOPIC, TEMPERATURE_TOPIC


def get_alerts(data):
    alerts = []
    if data["temperature"] > 40:
        alerts.append((TEMPERATURE_TOPIC, {**data, "alert_type": "temperature",
                       "message": "Temperature is above 40 C"}))
    if data["humidity"] > 80 or data["humidity"] < 20:
        alerts.append((HUMIDITY_TOPIC, {**data, "alert_type": "humidity",
                       "message": "Humidity is outside 20-80 %"}))
    return alerts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default=f"{PREFIX}_sensor_filter")
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument("--idle-timeout", type=int, default=0, help="Seconds; 0 means no timeout")
    args = parser.parse_args()
    consumer = KafkaConsumer(
        SENSORS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=args.idle_timeout * 1000 if args.idle_timeout else float("inf"),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )
    processed = 0
    try:
        for message in consumer:
            data = message.value
            print(json.dumps({"event": "RECEIVED", "partition": message.partition,
                              "offset": message.offset, "data": data}), flush=True)
            alerts = get_alerts(data)
            print(f"FILTER id={data['id']} alerts={len(alerts)}", flush=True)
            for topic, alert in alerts:
                metadata = producer.send(topic, key=str(data["id"]), value=alert).get(timeout=15)
                print(json.dumps({"event": "ALERT_SENT", "topic": metadata.topic,
                                  "partition": metadata.partition, "offset": metadata.offset,
                                  "data": alert}), flush=True)
            # Commit only after all output messages were acknowledged by Kafka.
            consumer.commit()
            processed += 1
            if args.max_messages and processed >= args.max_messages:
                break
    except KeyboardInterrupt:
        print("Filter stopped", flush=True)
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
