package org.ruppyrup.twitter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;

public class TwitterProcessor {

  public static void main(String[] args) throws Exception {
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    SparkSession session = SparkSession.builder()
                               .master("local[*]")
                               .appName("structuredViewingReport")
                               .getOrCreate();

    StructType schema = new StructType()
                            .add("created_at", DataTypes.DateType, true)
                            .add("id", DataTypes.LongType, true)
                            .add("id_str", DataTypes.StringType, true)
                            .add("text", DataTypes.StringType, true);


    Dataset<Row> df = session.readStream()
                          .format("kafka")
                          .option("kafka.bootstrap.servers", "localhost:9092")
                          .option("subscribe", "twitter_tweets")
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

    Dataset<Row> stringDataset = df
                                     .select(col("value").cast(DataTypes.StringType));


//    StreamingQuery console = results
//                                 .writeStream()
//                                 .format("console")
//                                 .outputMode(OutputMode.Complete())
//                                 .start();

    StreamingQuery console = stringDataset
                                 .writeStream()
                                 .format("console")
                                 .option("truncate", false)
                                 .outputMode(OutputMode.Update())
                                 .start();

    console.awaitTermination();

  }
}
