package org.ruppyrup.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class ViewingFromKafka {

  public static void main(String[] args) throws InterruptedException {

    long duration = 10;
    Logger.getRootLogger().setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]");

    JavaStreamingContext sparkStreamingContext = new JavaStreamingContext(conf, Durations.seconds(duration));

    List<String> topics = Arrays.asList("viewrecords");

    Map<String, Object> params = Map.of(
        "bootstrap.servers", "localhost:9092,anotherhost:9092",
        "key.deserializer", StringDeserializer.class,
        "value.deserializer", StringDeserializer.class,
        "group.id", "spark_group",
        "auto.offset.reset", "latest");
//        "enable.auto.commit", true);

    JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
        sparkStreamingContext,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.Subscribe(topics, params)
    );

    JavaPairDStream<Long, String> results = stream.mapToPair(data -> new Tuple2<>(data.value(), 5L))
        .reduceByKeyAndWindow(Long::sum, Durations.minutes(60),  Durations.minutes(1))
        .mapToPair(Tuple2::swap)
        .transformToPair(rdd -> rdd.sortByKey(false));

    results.print(50);

    sparkStreamingContext.start();
    sparkStreamingContext.awaitTermination();
  }

}
