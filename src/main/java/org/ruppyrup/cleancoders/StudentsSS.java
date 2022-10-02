package org.ruppyrup.cleancoders;

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

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.stddev;
import static org.apache.spark.sql.functions.sum;

public class StudentsSS {

  public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    SparkSession session = SparkSession.builder()
                               .master("local[*]")
                               .appName("structuredViewingReport")
                               .getOrCreate();

    // speeds everything up as limits number of partitions
//    session.conf().set("spark.sql.shuffle.partitions", "10");

    Dataset<Row> sourceData = session.readStream()
                                  .format("rate")
                                  .option("rowsPerSecond", 1)
                                  .load();

    session.udf().register("isEven", (Long value) -> value % 2 == 0, DataTypes.BooleanType);


    Dataset<Row> agg = sourceData
                           .withColumn("even", callUDF("isEven", col("value")));


    StreamingQuery console = agg
                                 .writeStream()
                                 .format("console")
                                 .option("truncate", false)
                                 .option("numRows", 50)
                                 .outputMode(OutputMode.Update())
                                 .start();

    console.awaitTermination();


  }

}
