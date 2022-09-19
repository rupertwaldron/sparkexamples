package org.ruppyrup.reconreplay;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class ReconProcessor {

  public static void main(String[] args) throws InterruptedException {

    Map<Integer, List<String>> dataMap = new ConcurrentHashMap<>();

    long duration = 2;
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("reconviewer").setMaster("local[*]");

    JavaStreamingContext sparkStreamingContext = new JavaStreamingContext(conf, Durations.seconds(duration));
    sparkStreamingContext.checkpoint(".");

    List<String> topics = Arrays.asList("reconreplay");

    Map<String, Object> params = Map.of(
        "bootstrap.servers", "localhost:9092,anotherhost:9092",
        "key.deserializer", IntegerDeserializer.class,
        "value.deserializer", StringDeserializer.class,
        "group.id", "spark_group",
        "auto.offset.reset", "latest",
        "enable.auto.commit", false);

    JavaInputDStream<ConsumerRecord<Integer, String>> stream = KafkaUtils.createDirectStream(
        sparkStreamingContext,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.Subscribe(topics, params)
    );

    Function2<List<String>, Optional<List<String>>, Optional<List<String>>> stateDataFunction = (values, state) -> {
      List<String> stateDataList = state.or(new ArrayList<>());
      stateDataList.addAll(values);
      return Optional.of(stateDataList);
    };

    JavaPairDStream<Integer, List<String>> listDataState = stream
        .mapToPair(item -> new Tuple2<>(item.key(), item.value()))
        .updateStateByKey(stateDataFunction);

    JavaPairDStream<Integer, Integer> dStream = stream
        .mapToPair(item -> new Tuple2<>(item.key(), 1))
        .reduceByKey(Integer::sum);

    Function2<List<Integer>, Optional<Integer>, Optional<Integer>> stateFunction = (values, state) -> {
      Integer sum = state.or(0);
      for(Integer value : values) {
        sum += value;
      }
      return Optional.of(sum);
    };

    JavaPairDStream<Integer, Integer> results = dStream.updateStateByKey(stateFunction);

    results.foreachRDD(rdd -> {
      rdd.filter(value -> value._2 == 20)
          .foreach(value -> {
            System.out.println(value._1 + " has reached 20");
          });
    });

    results.print();

    sparkStreamingContext.start();
    sparkStreamingContext.awaitTermination();
  }
}
