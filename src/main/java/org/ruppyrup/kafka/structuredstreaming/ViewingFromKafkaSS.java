package org.ruppyrup.kafka.structuredstreaming;

import java.util.concurrent.TimeoutException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class ViewingFromKafkaSS {

  public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    SparkSession session = SparkSession.builder()
        .master("local[*]")
        .appName("structuredViewingReport")
        .getOrCreate();

    Dataset<Row> df = session.readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "viewrecords")
        .load();


    df.createOrReplaceTempView("viewing_figures");

    // key, value, timestamp
    Dataset<Row> results =
        session.sql("select cast (value as string) as course_name, sum(5) from viewing_figures group by course_name");

    StreamingQuery query = results.writeStream()
        .format("console")
        .outputMode(OutputMode.Complete())
        .start();

    query.awaitTermination();

  }

}
