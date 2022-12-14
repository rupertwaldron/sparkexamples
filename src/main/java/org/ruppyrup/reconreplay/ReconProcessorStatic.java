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
import org.apache.spark.streaming.api.java.JavaDStream;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ReconProcessorStatic {

  private static final Map<Integer, ReconUnit> staticState = new ConcurrentHashMap<>();
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  public static void main(String[] args) throws InterruptedException {

    long duration = 1;
    int windowSize = 10;
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

    Function3<Integer, Optional<Event>, State<ReconUnit>, Tuple2<Integer, Optional<ReconResult>>> staticStateFunction = (key, values, state) -> {

      ReconUnit reconUnit = staticState.computeIfAbsent(key, k -> {
        ReconUnit ru1 = new ReconUnit(windowSize);
        scheduler.schedule(() -> processTimeout(k), 5, TimeUnit.SECONDS);
        return ru1;
      });


      if (reconUnit.hasTimedOut()) {
        System.out.println("Timeout Hashmap size = " + staticState.size());
        staticState.remove(key);
        return new Tuple2<>(key, Optional.of(new ReconResult(reconUnit, "Timed out")));
      }


//      printMemory();

      if (values.isPresent()) {
        reconUnit.addEvent(values.get());
        staticState.put(key, reconUnit);
      }

      if (!reconUnit.isComplete()) {
        System.out.println("Hashmap size = " + staticState.size());
        return new Tuple2<>(key, Optional.empty());
      } else {
        staticState.remove(key);
        System.out.println("Hashmap size = " + staticState.size());
        return new Tuple2<>(key, Optional.of(new ReconResult(reconUnit, "Completed")));
      }
    };

    JavaDStream<Tuple2<Integer, Optional<ReconResult>>> reconResults = createTimeoutStreams(stream, staticStateFunction);

//    JavaPairDStream<Integer, ReconResult> processedEvents = reconResults
//                                                                                  .mapWithState(StateSpec.function(staticStateFunction))
//                                                                                  .filter(item -> item._2.isPresent())
//                                                                                  .mapToPair(item -> new Tuple2<>(item._1, item._2.get()));

    JavaPairDStream<Integer, ReconResult> timedOutEvents = reconResults
                                                                .filter(item -> item._2.get().getReconUnit().hasTimedOut())
                                                                .mapToPair(item -> new Tuple2<>(item._1, item._2.get()));

    JavaPairDStream<Integer, ReconResult> processedEvents = reconResults
                                                               .filter(item -> !item._2.get().getReconUnit().hasTimedOut())
                                                               .mapToPair(item -> new Tuple2<>(item._1, item._2.get()));


//        .mapWithState(StateSpec.function(mapWithStateFunction).timeout(Durations.seconds(30)));
////        .filter(item -> !item._2.isEmpty());

//    JavaPairDStream<Integer, Integer> integerIntegerJavaPairDStream = resultsStream
//                                                                          .filter(item -> item._2.isComplete())
//                                                                          .transform(rows -> rows.distinct())
//                                                                          .mapToPair(item -> new Tuple2<>(item._1, item._2.getEventCount()));
    processedEvents.print(50);
    timedOutEvents.print(50);

//    integerIntegerJavaPairDStream.print();

//    integerIntegerJavaPairDStream.foreachRDD(rdd -> System.out.println(rdd.toDebugString()));

    sparkStreamingContext.start();
    sparkStreamingContext.awaitTermination();
  }

  private static JavaDStream<Tuple2<Integer, Optional<ReconResult>>> createTimeoutStreams(
      final JavaInputDStream<ConsumerRecord<Integer, String>> stream, final Function3<Integer, Optional<Event>, State<ReconUnit>, Tuple2<Integer, Optional<ReconResult>>> mapWithStateFunction) {

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

    return eventsStream
               .mapWithState(StateSpec.function(mapWithStateFunction))
               .filter(item -> item._2.isPresent());

  }

  private static void printMemory() {
    Runtime runtime = Runtime.getRuntime();
    long totalMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();

    System.out.println("Memory: Used=" + (totalMemory - freeMemory) + " Total=" + totalMemory + " Free=" + freeMemory);
  }

  private static void processTimeout(Integer windowId) {
    ReconUnit reconUnit = staticState.get(windowId);
    System.out.println("Processing timeout of window Id :: " + windowId + " reconunit = " + reconUnit);
    if (reconUnit != null)
      reconUnit.setTimedOut();
  }
}
