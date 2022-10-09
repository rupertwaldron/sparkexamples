package org.ruppyrup.reconwithss;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class SSReconProcessor {

  static final int WINDOW_SIZE = 100;

  public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {

    // speeds everything up as limits number of partitions

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
                               .config("spark.sql.streaming.checkpointLocation", "./rdds")
                               .getOrCreate();

    session.conf().set("spark.sql.shuffle.partitions", "10");

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
    Encoder<Tuple3<Integer, Integer, AccountWindow>> tuple3Encoder = Encoders.tuple(Encoders.INT(), Encoders.INT(), listEncoder);
    Dataset<Tuple3<Integer, Integer, AccountWindow>> windowResultDataset = groupByWindowId.mapGroupsWithState((MapGroupsWithStateFunction<Integer, AccountWrapper, AccountWindow, Tuple3<Integer, Integer, AccountWindow>>) (key, values, state) -> {

      if (!values.hasNext() || state.hasTimedOut()) {
        System.out.println("State timeout for key -> " + key);
        state.remove();
        return new Tuple3<>(-1, -1, new AccountWindow());
      }

      AccountWindow currentWindowState = state.getOption().getOrElse(AccountWindow::new);

      while (values.hasNext()) {
        AccountWrapper accountWrapper = values.next();
        currentWindowState.getAccountWrappers().add(accountWrapper);
      }
      state.update(currentWindowState);

      if (currentWindowState.getAccountWrappers().size() == WINDOW_SIZE) {
        System.out.println("Removing state for key -> " + key);
        state.remove();
      }

      return new Tuple3<>(currentWindowState.getAccountWrappers().size(), key, currentWindowState);
    }, listEncoder, tuple3Encoder);

    FlatMapFunction<Tuple3<Integer, Integer, AccountWindow>, AccountWrapper> tuple3AccountWrapperFlatMapFunction = tpl3 ->
                                                                                                                       tpl3._3().getAccountWrappers().iterator();

    StreamingQuery console = windowResultDataset
                                 .filter(col("_1").equalTo(WINDOW_SIZE))
                                 .flatMap(tuple3AccountWrapperFlatMapFunction, Encoders.bean(AccountWrapper.class))
                                 .writeStream()
                                 .format("console")
                                 .option("truncate", false)
                                 .outputMode(OutputMode.Update())
                                 .start();

    console.awaitTermination();


  }

}
