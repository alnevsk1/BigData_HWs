Данная ДЗ состоит из двух файлов. Вначале запускается producer.py:

```bash
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
python producer.py
```

Затем spark_consumer.py

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark_consumer.py
```

В файле producer.py следует вписать свои данные в переменные `API_ID` и `API_HASH`. Для успешной работы необходимо вступить во все каналы, id которых указаны в массиве `CHANNEL_IDS.`

Результат выполнения представлен в папке `output/batch10.`
