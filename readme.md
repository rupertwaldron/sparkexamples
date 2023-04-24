Start up Zookeeper:
```bash
zookeeper-server-start zoo.cfg
```

Start up Kafka:
```bash
/usr/local/bin/kafka-server-start /usr/local/etc/kafka/server.properties
```

Create the relavent topic on kafka:
```bash
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic replay
```

Check the topic has been created:
```bash
kafka-topics --list --bootstrap-server localhost:9092
```

To view Spark running in the web brower:
http://localhost:4040/jobs/
