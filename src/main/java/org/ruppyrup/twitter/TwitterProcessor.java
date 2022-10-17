package org.ruppyrup.twitter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

public class TwitterProcessor {

  public static void main(String[] args) throws Exception {
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    SparkSession session = SparkSession.builder()
                               .master("local[*]")
                               .appName("structuredTwitter")
                               .config("spark.sql.streaming.checkpointLocation", "./rdds")
                               .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                               .config("spark.sql.shuffle.partitions", "10")
                               .getOrCreate();

    StructType twitterSchema = new StructType()
                                   .add("created_at", DataTypes.StringType, true)
                                   .add("id", DataTypes.LongType, true)
                                   .add("id_str", DataTypes.StringType, true)
                                   .add("text", DataTypes.StringType, true);


    Dataset<TwitterDto> df = session.readStream()
                                 .format("kafka")
                                 .option("kafka.bootstrap.servers", "localhost:9092")
                                 .option("subscribe", "twitter_tweets")
                                 .load()
                                 .selectExpr("CAST(value AS STRING) as message")
                                 .select(from_json(col("message"), twitterSchema).as("json"))
                                 .select("json.*")
                                 .as(Encoders.bean(TwitterDto.class));


    session.udf().register("hashtag", (String text) -> {
      String[] split = text.split(" ");
      for (String s : split) {
        if (s.startsWith("@")) {
          return s;
        }
      }
      return "No tag";
    }, DataTypes.StringType);
    Dataset<Row> javaApi = df
                               .withColumn("timestamp", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss Z yyyy"))
                               .withColumn("hashtag", callUDF("hashtag", col("text")))
                               .filter(col("hashtag").notEqual("No tag"))
                               .withWatermark("timestamp", "10 hours") // for use with windowing so discard old data and don't hold state
                               .groupBy(
                                   functions.window(col("timestamp"), "5 minutes"),
                                   col("hashtag"))
                               .count()
//                               .orderBy(col("window.start").desc(), col("count").desc())
//                               .sort(desc("window"))
                               .sort(desc("count"))
                               .limit(20);
//                               .agg(
//                                   count(col("hashtag")).alias("@ Count")
//                               )
//                               .sort(desc("@ Count"))
//                               .limit(20);


//
//    Dataset<Row> stringDataset = df
//                                     .select(col("value").cast(DataTypes.StringType));


    StreamingQuery console = javaApi
                                 .writeStream()
                                 .format("console")
                                 .option("truncate", false)
                                 .trigger(Trigger.ProcessingTime(5, TimeUnit.MINUTES))
//                                 .option("numrows", 20)
                                 .outputMode(OutputMode.Complete())
                                 .start();

    console.awaitTermination();

  }
}
