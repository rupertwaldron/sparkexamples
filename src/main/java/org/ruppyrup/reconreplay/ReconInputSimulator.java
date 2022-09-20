package org.ruppyrup.reconreplay;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;

public class ReconInputSimulator {

  public static void main(String[] args) {
    Producer<Integer, String> producer = createProducer();
    List<CompletableFuture<Void>> cfs = new ArrayList<>();

    long sendMessageCount = 20;

    String topic = "reconreplay";

    for (int i = 100; i < 2000; i++) {

      int id= i;
      cfs.add(CompletableFuture.runAsync(() -> {
        try {
          publishData(id, producer, sendMessageCount, topic);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));

    }

    cfs.forEach(CompletableFuture::join);

    producer.flush();
    producer.close();

  }

  private static void publishData(int windowId, Producer<Integer, String> producer, long sendMessageCount,
      String topic)
      throws InterruptedException, ExecutionException {
    for (int count = 0; count < sendMessageCount; count++) {

      ProducerRecord<Integer, String> dataToSend = new ProducerRecord<>(topic, windowId,
          Thread.currentThread().getName() + " :: " + count);

      RecordMetadata metadata = producer.send(dataToSend).get();

      System.out.printf("sent dataToSend(key=%s value=%s) " +
              "meta(partition=%d, offset=%d)\n",
          dataToSend.key(), dataToSend.value(), metadata.partition(), metadata.offset());

      Thread.sleep((long) (Math.random() * 10));
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
