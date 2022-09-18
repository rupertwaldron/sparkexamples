package org.ruppyrup.streaming.datasource;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class LogStreamAnalysis {

  @SuppressWarnings("resource")
  public static void main(String[] args) throws InterruptedException {

    Logger.getRootLogger().setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]");

    JavaStreamingContext sparkStreamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

    JavaReceiverInputDStream<String> inputData = sparkStreamingContext.socketTextStream("localhost", 8989);

    JavaDStream<String> results = inputData.map(item -> item);
    JavaPairDStream<String, Long> logCounter = results.mapToPair(raw -> new Tuple2<>(raw.split(",")[0], 1L));
    JavaPairDStream<String, Long> logSum = logCounter.reduceByKeyAndWindow(Long::sum, Durations.minutes(2));

    logSum.print();

    sparkStreamingContext.start();
    sparkStreamingContext.awaitTermination();
  }

}
