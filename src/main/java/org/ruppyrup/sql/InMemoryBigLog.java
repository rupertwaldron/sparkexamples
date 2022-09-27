package org.ruppyrup.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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


/**
 * This class is used in the chapter late in the course where we analyse viewing figures. You can ignore until then.
 */
public class InMemoryBigLog {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    SparkSession sqlspark = SparkSession.builder()
                                .appName("sqlspark")
                                .master("local[*]")
                                .getOrCreate();

    Dataset<Row> dataset = sqlspark.read().option("header", true).csv("src/main/resources/data/sql/biglog.txt");
    dataset.createOrReplaceTempView("logging_table");

    //Need agregation function like count, otherwise would be too many records
//    Dataset<Row> results = sqlspark.sql("select level, count(datetime) from logging_table group by level order by level");
//    Dataset<Row> results = sqlspark.sql("select level, collect_list(datetime) from logging_table group by level order by level");
    Dataset<Row> results = sqlspark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table");

    results.createOrReplaceTempView("logging_table");

    Dataset<Row> groupResults = sqlspark.sql("select level, month, count(1) as total from logging_table group by level, month");

    groupResults.show(100);

    groupResults.createOrReplaceTempView("results_table");

    Dataset<Row> totalRows = sqlspark.sql("select sum(total) from results_table");

    totalRows.show();

    sqlspark.close();
  }
}
