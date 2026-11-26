"""Run with the cluster Python containing kafka-python; never changes source data."""
import argparse
import json
import time
from kafka import KafkaProducer

p = argparse.ArgumentParser()
p.add_argument('--broker', required=True)
p.add_argument('--file', required=True)
p.add_argument('--run-id', required=True)
p.add_argument('--topic', default='movielens-live-ratings')
p.add_argument('--rate', type=int, default=100)
p.add_argument('--limit', type=int, default=30000)
a = p.parse_args()
if a.rate <= 0 or a.limit <= 0:
    p.error('rate and limit must be positive')
producer = KafkaProducer(bootstrap_servers=a.broker, acks='all',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
try:
    started = time.monotonic()
    with open(a.file, encoding='ascii') as source:
        for seq, line in enumerate(source):
            if seq >= a.limit:
                break
            user, movie, rating, timestamp = map(int, line.strip().split('::'))
            if user <= 0 or movie <= 0 or not 1 <= rating <= 5 or timestamp <= 0:
                raise ValueError('Invalid rating at row %s' % seq)
            producer.send(a.topic, key=a.run_id.encode(), value=dict(run_id=a.run_id,
                event_seq=seq, user_id=user, movie_id=movie, rating=rating, timestamp=timestamp)).get(timeout=15)
            time.sleep(max(0, started + (seq + 1) / a.rate - time.monotonic()))
            if (seq + 1) % a.rate == 0:
                print('sent=%s run=%s' % (seq + 1, a.run_id), flush=True)
finally:
    producer.flush()
    producer.close()
