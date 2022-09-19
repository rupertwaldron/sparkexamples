package org.ruppyrup.reconreplay;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;

public class ReconInputSimulator {

  public static void main(String[] args) {
    Producer<String, String> producer = createProducer();

    long sendMessageCount = 20;

    String topic = "reconreplay";

    CompletableFuture<Void> publishCF = CompletableFuture.runAsync(() -> {
      try {
        publishData("101", producer, sendMessageCount, topic);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    CompletableFuture<Void> publishCF2 = CompletableFuture.runAsync(() -> {
      try {
        publishData("102", producer, sendMessageCount, topic);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    publishCF.join();
    publishCF2.join();
    producer.flush();
    producer.close();

  }

  private static void publishData(String windowId, Producer<String, String> producer, long sendMessageCount, String topic)
      throws InterruptedException, ExecutionException {
    for (int count = 0; count < sendMessageCount; count++) {

      ProducerRecord<String, String> dataToSend = new ProducerRecord<>(topic, windowId + "::" + count,
          Thread.currentThread().getName() + " :: " + count);

      RecordMetadata metadata = producer.send(dataToSend).get();

      System.out.printf("sent dataToSend(key=%s value=%s) " +
              "meta(partition=%d, offset=%d)\n",
          dataToSend.key(), dataToSend.value(), metadata.partition(), metadata.offset());

      Thread.sleep((long) (Math.random() * 1000));
    }
  }

  @NotNull
  private static Producer<String, String> createProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all"); // See https://kafka.apache.org/documentation/
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<>(props);
  }
}
