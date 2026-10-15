import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import BOOTSTRAP_SERVERS, HUMIDITY_TOPIC, PREFIX, SENSORS_TOPIC, TEMPERATURE_TOPIC


def main():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        for name in (SENSORS_TOPIC, TEMPERATURE_TOPIC, HUMIDITY_TOPIC):
            try:
                admin_client.create_topics(
                    [NewTopic(name=name, num_partitions=2, replication_factor=1)]
                )
                print(f"CREATED {name}", flush=True)
            except TopicAlreadyExistsError:
                print(f"EXISTS {name}", flush=True)
        # Topic creation can complete before every broker refreshes its metadata.
        required = {SENSORS_TOPIC, TEMPERATURE_TOPIC, HUMIDITY_TOPIC}
        for _ in range(20):
            if required.issubset(admin_client.list_topics()):
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("Created topics did not appear in Kafka metadata")
        my_name = PREFIX
        [print(topic) for topic in sorted(admin_client.list_topics()) if my_name in topic]
    finally:
        admin_client.close()


if __name__ == "__main__":
    main()
