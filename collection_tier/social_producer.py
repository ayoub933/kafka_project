import time
import json
import random
import os
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime, timedelta

# Configuration similaire aux exercices précédents
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "social_data"

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    fake = Faker()
    
    print(f"Démarrage de l'envoi vers le topic: {TOPIC_NAME}...")

    # Mots clés pour simuler des sentiments
    POS_WORDS = ["awesome", "great", "love", "fantastic", "happy"]
    NEG_WORDS = ["terrible", "bad", "hate", "awful", "sad", "angry"]

    try:
        while True:
            # Simulation de données
            platform = random.choice(["Twitter", "Reddit"])
            sentiment_type = random.choice(["pos", "neg", "neutral"])
            
            if sentiment_type == "pos":
                text = f"{fake.sentence()} {random.choice(POS_WORDS)}!"
            elif sentiment_type == "neg":
                text = f"{fake.sentence()} {random.choice(NEG_WORDS)}."
            else:
                text = fake.sentence()

            # Création de la donnée (Event Time pour le TD)
            # On simule parfois un léger retard (late data) pour le watermarking
            event_time = datetime.now() - timedelta(seconds=random.randint(0, 5))

            data = {
                "id": fake.uuid4(),
                "platform": platform,
                "text": text,
                "creation_time": event_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Envoi au topic
            producer.send(TOPIC_NAME, value=data)
            print(f"Envoyé: {data['creation_time']} | {platform}")
            
            # Pause pour simuler un flux
            time.sleep(1.0)
            
    except KeyboardInterrupt:
        print("Arrêt du producer.")
        producer.close()