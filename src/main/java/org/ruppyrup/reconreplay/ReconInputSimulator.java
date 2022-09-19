package org.ruppyrup.reconreplay;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;

public class ReconInputSimulator {

  public static void main(String[] args) throws InterruptedException, FileNotFoundException {
    Producer<Integer, String> producer = createProducer();
    long time = System.currentTimeMillis();
    long sendMessageCount = 1000;

    String topic = "reconreplay";

    try {
      for (int count = 0; count < sendMessageCount; count++) {

        ProducerRecord<Integer, String> dataToSend = new ProducerRecord<>(topic, count,
            "Hello Mom " + count);

        RecordMetadata metadata = producer.send(dataToSend).get();

        long elapsedTime = System.currentTimeMillis() - time;
        System.out.printf("sent dataToSend(key=%s value=%s) " +
                "meta(partition=%d, offset=%d) time=%d\n",
            dataToSend.key(), dataToSend.value(), metadata.partition(),
            metadata.offset(), elapsedTime);
        Thread.sleep(1000);

      }
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } finally {
      producer.flush();
      producer.close();
    }
  }

  @NotNull
  private static Producer<Integer, String> createProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all"); // See https://kafka.apache.org/documentation/
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<>(props);
  }
}
