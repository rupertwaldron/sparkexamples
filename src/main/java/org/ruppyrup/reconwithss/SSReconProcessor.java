package org.ruppyrup.reconwithss;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class SSReconProcessor {

  private static final int WINDOW_SIZE = 10;

  public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    StructType accountSchema = DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("accountNumber", DataTypes.StringType, true),
        DataTypes.createStructField("accountName", DataTypes.StringType, true),
        DataTypes.createStructField("balance", DataTypes.DoubleType, true),
    });

    //create schema for json message
    StructType accountWrapperSchema = DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("windowId", DataTypes.IntegerType, true),
        DataTypes.createStructField("counter", DataTypes.IntegerType, true),
        DataTypes.createStructField("account", accountSchema, true),
    });

    SparkSession session = SparkSession.builder()
                               .master("local[*]")
                               .appName("structuredreconreplay")
                               .getOrCreate();


    Dataset<AccountWrapper> df = session.readStream()
                                     .format("kafka")
                                     .option("kafka.bootstrap.servers", "localhost:9092")
                                     .option("subscribe", "reconreplay")
                                     .load()
                                     .selectExpr("CAST(value AS STRING) as message")
                                     .select(functions.from_json(col("message"), accountWrapperSchema).as("json"))
                                     .select("json.*")
                                     .as(Encoders.bean(AccountWrapper.class));

    KeyValueGroupedDataset<Integer, AccountWrapper> groupByWindowId = df.groupByKey((MapFunction<AccountWrapper, Integer>) AccountWrapper::getWindowId, Encoders.INT());


    Encoder<AccountWindow> listEncoder = Encoders.bean(AccountWindow.class);
    Encoder<Tuple2<Integer, AccountWindow>> tuple2Encoder = Encoders.tuple(Encoders.INT(), listEncoder);
    Dataset<Tuple2<Integer, AccountWindow>> windowResultDataset = groupByWindowId.mapGroupsWithState((MapGroupsWithStateFunction<Integer, AccountWrapper, AccountWindow, Tuple2<Integer, AccountWindow>>) (key, values, state) -> {

      if (!values.hasNext() || state.hasTimedOut()) {
        System.out.println("State timeout for key -> " + key);
        state.remove();
        return new Tuple2<>(-1, new AccountWindow());
      }

      AccountWindow currentWindowState = state.getOption().getOrElse(AccountWindow::new);
      currentWindowState.setWindowId(key);

      while (values.hasNext()) {
        AccountWrapper accountWrapper = values.next();
        currentWindowState.getAccountWrappers().add(accountWrapper);
      }
      state.update(currentWindowState);

      if (currentWindowState.getAccountWrappers().size() == 10) {
        System.out.println("Removing state for key -> " + key);
        state.remove();
      }

      return new Tuple2<>(currentWindowState.getAccountWrappers().size(), currentWindowState);
    }, listEncoder, tuple2Encoder);


//    Dataset<Row> javaApi = df
//                               .withColumn("total", lit(5))
//                               .select(
//                                   col("value").cast(DataTypes.StringType).alias("course_name"),
//                                   col("total")
//                               )
//                               .groupBy(col("course_name"))
////                               .pivot(col("total"))
//                               .agg(
//                                   round(sum(col("total")), 2).alias("score")
//                               )
//                               .sort(desc("score"));


    StreamingQuery console = windowResultDataset
                                 .select(col("_1").alias("count"), col("_2").alias("data"))
                                 .filter(col("count").equalTo(10))
                                 .writeStream()
                                 .format("console")
                                 .option("truncate", false)
                                 .outputMode(OutputMode.Update())
                                 .start();

    console.awaitTermination();


  }

}
