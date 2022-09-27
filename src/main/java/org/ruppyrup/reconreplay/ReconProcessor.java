package org.ruppyrup.reconreplay;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class ReconProcessor {

  public static void main(String[] args) throws InterruptedException {

    long duration = 1;
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("reconviewer").setMaster("local[*]");

    JavaStreamingContext sparkStreamingContext = new JavaStreamingContext(conf, Durations.seconds(duration));
    sparkStreamingContext.checkpoint("./rdds");

    List<String> topics = Arrays.asList("reconreplay");

    Map<String, Object> params = Map.of(
        "bootstrap.servers", "localhost:9092",
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

    Function3<Integer, Optional<String>, State<List<String>>, Tuple2<Integer, List<String>>> mapWithStateFunction = (key, values, state) -> {
      if (!state.exists()) {
        state.update(new ArrayList<>());
      }

      List<String> stateDataList = state.get();
//      System.out.println("State list size for key " + key + " :: " + stateDataList.size());

      if (values.isPresent()) {
        stateDataList.add(values.get());
        state.update(stateDataList);
      }
      return new Tuple2<>(key, stateDataList);
    };

    var dstreamWithState = stream
        .mapToPair(item -> new Tuple2<>(item.key(), item.value()))
//        .mapWithState(StateSpec.function(mapWithStateFunction));
        .mapWithState(StateSpec.function(mapWithStateFunction).timeout(Durations.seconds(30)));
//        .filter(item -> !item._2.isEmpty());

    JavaPairDStream<Integer, Integer> integerIntegerJavaPairDStream = dstreamWithState
        .filter(item -> item._2.size() == 20)
        .transform(rows -> rows.distinct())
        .mapToPair(item -> new Tuple2<>(item._1, item._2.size()));

    integerIntegerJavaPairDStream.print();

//    integerIntegerJavaPairDStream.foreachRDD(rdd -> System.out.println(rdd.toDebugString()));

    sparkStreamingContext.start();
    sparkStreamingContext.awaitTermination();
  }
}
