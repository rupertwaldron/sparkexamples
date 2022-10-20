package org.ruppyrup.reconwithss;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.ruppyrup.reconwithss.SSReconProcessor.WINDOW_SIZE;

public class SSReconInputSimulator {

  private static final Random rand = new Random();
  private static final ObjectMapper mapper = new ObjectMapper();

  public static void main(String[] args) {



    Producer<Integer, String> producer = createProducer();
    List<CompletableFuture<Void>> cfs = new ArrayList<>();

    long sendMessageCount = WINDOW_SIZE;

    String topic = "reconreplay";

    for (int i = 1; i < 10001; i++) {

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
      throws InterruptedException, ExecutionException, JsonProcessingException {
    for (int count = 0; count < sendMessageCount; count++) {

      Account account = new Account(
          String.valueOf(rand.nextInt(1000_000)),
          "Rupert",
          100 * rand.nextDouble()
      );

      AccountWrapper wrapper = new AccountWrapper(account, windowId, count);

      ProducerRecord<Integer, String> dataToSend = new ProducerRecord<>(topic, windowId,
          mapper.writeValueAsString(wrapper));

      RecordMetadata metadata = producer.send(dataToSend).get();

      System.out.printf("sent dataToSend(key=%s value=%s) " +
              "meta(partition=%d, offset=%d)\n",
          dataToSend.key(), dataToSend.value(), metadata.partition(), metadata.offset());

      Thread.sleep(rand.nextInt(1));
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
