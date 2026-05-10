
from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

history = defaultdict(list)

print("Szukam użytkowników z powyżej 3 transakcjami w ciagu ostatnich 60 sekund")

for message in consumer:
    
    tx = message.value
    u_id = tx['user_id']
    current_time = datetime.fromisoformat(tx['timestamp']).timestamp()

    history[u_id].append(current_time)
    history[u_id] = [t for t in history[u_id] if current_time - t <= 60]

    if len(history[u_id]) > 3:
        print(f" ANOMALIA PRĘDKOŚCI !! Klient: {u_id}")
        print(f"Zbyt duża częstotliwość: {len(history[u_id])} transakcji w ostatniej minucie.")
        