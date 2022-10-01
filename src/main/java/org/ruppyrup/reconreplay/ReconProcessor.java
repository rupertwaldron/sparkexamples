package org.ruppyrup.reconreplay;


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
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ReconProcessor {

  public static void main(String[] args) throws InterruptedException {

    long duration = 1;
    int windowSize = 100;
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

    Function3<Integer, Optional<Event>, State<ReconUnit>, Tuple2<Integer, Optional<ReconResult>>> mapWithStateFunction = (key, values, state) -> {

      ReconUnit reconUnit = state.getOption().getOrElse(() -> new ReconUnit(windowSize));

      if (state.isTimingOut()) {
        ReconResult reconResult = new ReconResult(reconUnit, key + " is timing out");
        return new Tuple2<>(key, Optional.of(reconResult));
      }

      if (values.isPresent()) {
        reconUnit.addEvent(values.get());
        state.update(reconUnit);
      }

      if (!reconUnit.isComplete()) {
        return new Tuple2<>(key, Optional.empty());
      } else {
        state.remove();
        return new Tuple2<>(key, Optional.of(new ReconResult(reconUnit, "Completed")));
      }
    };
    List<JavaPairDStream<Integer, ReconResult>> timeoutStream = new ArrayList<>();

    createTimeoutStreams(stream, mapWithStateFunction, timeoutStream);


//        .mapWithState(StateSpec.function(mapWithStateFunction).timeout(Durations.seconds(30)));
////        .filter(item -> !item._2.isEmpty());

//    JavaPairDStream<Integer, Integer> integerIntegerJavaPairDStream = resultsStream
//                                                                          .filter(item -> item._2.isComplete())
//                                                                          .transform(rows -> rows.distinct())
//                                                                          .mapToPair(item -> new Tuple2<>(item._1, item._2.getEventCount()));
    timeoutStream.forEach(stuff-> stuff.print(50));

//    integerIntegerJavaPairDStream.print();

//    integerIntegerJavaPairDStream.foreachRDD(rdd -> System.out.println(rdd.toDebugString()));

    sparkStreamingContext.start();
    sparkStreamingContext.awaitTermination();
  }

  private static void createTimeoutStreams(
      final JavaInputDStream<ConsumerRecord<Integer, String>> stream, final Function3<Integer, Optional<Event>, State<ReconUnit>, Tuple2<Integer, Optional<ReconResult>>> mapWithStateFunction,
      final List<JavaPairDStream<Integer, ReconResult>> timeoutStreams) {
    List.of(300, 600, 900).forEach(timeout -> {
      JavaPairDStream<Integer, Event> eventsStream = stream
                                                         .transformToPair(rdd -> rdd.mapPartitionsToPair(iterator -> {
                                                               final List<Tuple2<Integer, Optional<Event>>> ret = new ArrayList<>();
                                                               iterator.forEachRemaining(consumerRecord -> ret.add(
                                                                   new Tuple2<>(consumerRecord.key(), Optional.of(new Event(consumerRecord)))
                                                               ));
                                                               return ret.iterator();
                                                             })
//                                                                                   .filter(v1 -> v1._2.isPresent())
                                                                                     .mapValues(Optional::get));

      var resultsStream = eventsStream
                              .mapWithState(StateSpec.function(mapWithStateFunction)
                                                .timeout(Durations.seconds(timeout)))
                              .filter(item -> item._2.isPresent())
                              .mapToPair(item -> new Tuple2<>(item._1, item._2.get()));


      timeoutStreams.add(resultsStream);

    });
  }
}
