package org.ruppyrup.structuredstreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;

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

    Dataset<Row> sourceData = session.readStream()
                                  .format("rate")
                                  .option("rowsPerSecond", 1)
                                  .load();


    df.createOrReplaceTempView("viewing_figures");
//    sourceData.createOrReplaceTempView("viewing_figures");

    // key, value, timestamp
    Dataset<Row> results =
        session.sql("select cast (value as string) as course_name, sum(5) as seconds_watched from viewing_figures group by course_name order by seconds_watched desc");

//    StreamingQuery query = results.writeStream()
//                               .format("console")
//                               .outputMode(OutputMode.Complete())
//                               .start();

    Dataset<Row> javaApi = df
                               .withColumn("total", lit(5))
                               .select(
                                   col("value").cast(DataTypes.StringType).alias("course_name"),
                                   col("total")
                               )
                               .groupBy(col("course_name"))
//                               .pivot(col("total"))
                               .agg(
                                   round(sum(col("total")), 2).alias("score")
                               )
                               .sort(desc("score"));


//    StreamingQuery console = results
//                                 .writeStream()
//                                 .format("console")
//                                 .outputMode(OutputMode.Complete())
//                                 .start();

    StreamingQuery console = results
                                 .writeStream()
                                 .format("console")
                                 .option("truncate", false)
                                 .outputMode(OutputMode.Complete())
                                 .start();

    console.awaitTermination();


  }

}
