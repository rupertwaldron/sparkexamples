package org.ruppyrup.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;


/**
 * This class is used in the chapter late in the course where we analyse viewing figures. You can ignore until then.
 */
public class InMemory {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    SparkSession sqlspark = SparkSession.builder()
                                .appName("sqlspark")
                                .master("local[*]")
                                .getOrCreate();

    List<Row> inMemory = new ArrayList<>();
    inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
    inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
    inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
    inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
    inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

    StructField[] fields = new StructField[] {
        new StructField("level", DataTypes.StringType, false, Metadata.empty()),
        new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
    };

    final StructType schema = new StructType(fields);
    Dataset<Row> dataSet = sqlspark.createDataFrame(inMemory, schema);

    dataSet.createOrReplaceTempView("logging_table");

    //Need agregation function like count, otherwise would be too many records
//    Dataset<Row> results = sqlspark.sql("select level, count(datetime) from logging_table group by level order by level");
//    Dataset<Row> results = sqlspark.sql("select level, collect_list(datetime) from logging_table group by level order by level");
    Dataset<Row> results = sqlspark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table");

    results.createOrReplaceTempView("logging_table");

    Dataset<Row> groupResults = sqlspark.sql("select level, month, count(1) as total from logging_table group by level, month");

    groupResults.show();

    sqlspark.close();
  }
}
