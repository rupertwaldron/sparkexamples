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

/**
 * Does the main processing for Recon Agent
 */
public class ReconProcessor {

    public static void main(String[] args) throws InterruptedException {

        long duration = 1;
        int windowSize = 10;
        Logger.getRootLogger().setLevel(Level.WARN);
        Logger.getLogger("org").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("reconviewer").setMaster("local[*]");

        JavaStreamingContext sparkStreamingContext = new JavaStreamingContext(conf, Durations.seconds(duration));
        sparkStreamingContext.checkpoint("./rdds");

        List<String> topics = Arrays.asList("reconreplay");

    /*
      Set up parms to get key and value off kafka topic
     */
        Map<String, Object> params = Map.of(
                "bootstrap.servers", "localhost:9092",
                "key.deserializer", IntegerDeserializer.class,
                "value.deserializer", StringDeserializer.class,
                "group.id", "spark_group",
                "auto.offset.reset", "latest",
                "enable.auto.commit", false);

        /*
          Create DStream
         */
        JavaInputDStream<ConsumerRecord<Integer, String>> stream = KafkaUtils.createDirectStream(
                sparkStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, params)
        );

        /*
         * Create a map with state function
         * Gets the state for the given key and updates with the latest set of values
         */
        Function3<Integer, Optional<Event>, State<ReconUnit>, Tuple2<Integer, Optional<ReconResult>>> mapWithStateFunction = (key, values, state) -> {

            ReconUnit reconUnit = state.getOption().getOrElse(() -> new ReconUnit(windowSize));

            if (state.isTimingOut()) {
                ReconResult reconResult = new ReconResult(reconUnit, key + " is timing out");
                return new Tuple2<>(key, Optional.of(reconResult));
            }

//      printMemory();

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


        JavaPairDStream<Integer, ReconResult> reconResults = createTimeoutStreams(stream, mapWithStateFunction);


//        .mapWithState(StateSpec.function(mapWithStateFunction).timeout(Durations.seconds(30)));
////        .filter(item -> !item._2.isEmpty());

//    JavaPairDStream<Integer, Integer> integerIntegerJavaPairDStream = resultsStream
//                                                                          .filter(item -> item._2.isComplete())
//                                                                          .transform(rows -> rows.distinct())
//                                                                          .mapToPair(item -> new Tuple2<>(item._1, item._2.getEventCount()));
        reconResults.print(50);

//    integerIntegerJavaPairDStream.print();

//    integerIntegerJavaPairDStream.foreachRDD(rdd -> System.out.println(rdd.toDebugString()));

        sparkStreamingContext.start();
        sparkStreamingContext.awaitTermination();
    }

    /**
     * Map the events by windowId
     * @param stream
     * @param mapWithStateFunction
     * @return
     */
    private static JavaPairDStream<Integer, ReconResult> createTimeoutStreams(
            final JavaInputDStream<ConsumerRecord<Integer, String>> stream,
            final Function3<Integer, Optional<Event>, State<ReconUnit>, Tuple2<Integer, Optional<ReconResult>>> mapWithStateFunction) {

        /*
         * Create a Pair of Window Id and event
         */
        JavaPairDStream<Integer, Event> eventsStream = stream
                .transformToPair(rdd -> rdd.mapPartitionsToPair(iterator -> {
                            final List<Tuple2<Integer, Optional<Event>>> ret = new ArrayList<>();
                            iterator.forEachRemaining(consumerRecord -> ret.add(
                                    new Tuple2<>(consumerRecord.key(), Optional.of(new Event(consumerRecord)))
                            ));
                            return ret.iterator();
                        })
                        .mapValues(Optional::get));

        return eventsStream
                .mapWithState(StateSpec.function(mapWithStateFunction)
                        .timeout(Durations.seconds(60)))
                .filter(item -> item._2.isPresent()) // will only take the completed event
                .mapToPair(item -> new Tuple2<>(item._1, item._2.get()));

    }

    private static void printMemory() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();

        System.out.println("Memory: Used=" + (totalMemory - freeMemory) + " Total=" + totalMemory + " Free=" + freeMemory);
    }
}
