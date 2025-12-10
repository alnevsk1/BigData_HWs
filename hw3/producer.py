# cd /opt/kafka
# bin/zookeeper-server-start.sh config/zookeeper.properties
# bin/kafka-server-start.sh config/server.properties
# python producer.py

import asyncio
from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from kafka import KafkaProducer
import json

# Настройки Telegram 
API_ID = 'скрыто' #  API ID приложения
API_HASH = 'скрыто' # API HASH приложения
SESSION_NAME = 'telegram_session'
CHANNEL_IDS = [
    -1001967505770, #4ch
    -1001288489154, #Topor1
    -1001966291562, #Topor2
    -1001101170442, #RIA
    -1001747110091, #Moscow Live
    -1002416194304, #Topor3
    -1001288481277, #Moscwach
    -1001135818819, #KB
    -1001754252633, #ToporLive
    -1001394050290  #RansheVsex
]

# Настройки Kafka 
KAFKA_TOPIC = 'telegram_messages'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Инициализация Kafka Producer 
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v.encode('utf-8')
)

# Инициализация Telegram Client 
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

@client.on(events.NewMessage(chats=CHANNEL_IDS))
async def handle_new_message(event):
    """
    Обработчик новых сообщений.
    Отправляет текст сообщения в топик Kafka.
    """
    message_text = event.message.text
    if message_text:
        print(f"Получено сообщение из канала {event.chat_id}: {message_text[:50]}...")
        # Отправка сообщения в Kafka
        producer.send(KAFKA_TOPIC, value=message_text)
        producer.flush() # Гарантируем доставку
        print(f"Сообщение отправлено в Kafka топик '{KAFKA_TOPIC}'")

async def main():
    """
    Основная функция для подключения к Telegram и обработки сообщений.
    """
    await client.start()
    print("Клиент Telegram запущен.")

    # Подключаемся к каналам
    for channel_id in CHANNEL_IDS:
        try:
            entity = await client.get_entity(channel_id)
            print(f"Успешно подключен к каналу:  (ID: {channel_id})")
        except Exception as e:
            print(f"Не удалось подключиться к каналу ID {channel_id}: {e}")

    print("Ожидание новых сообщений...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Программа завершена.")
    finally:
        producer.close()
        print("Соединение с Kafka закрыто.")

