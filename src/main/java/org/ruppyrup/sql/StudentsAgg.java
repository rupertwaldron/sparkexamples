package org.ruppyrup.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;


/**
 * This class is used in the chapter late in the course where we analyse viewing figures. You can ignore until then.
 */
public class StudentsAgg {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);
//		SparkConf conf = new SparkConf().setAppName("sqlspark").setMaster("local[*]");
    SparkSession sqlspark = SparkSession.builder()
                                .appName("sqlspark")
                                .master("local[*]")
                                .getOrCreate();

    // inferSchema goes over the data twice so can be very slow - do manual cast instead
//    Dataset<Row> dataset = sqlspark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/data/sql/exams/students.csv");
    Dataset<Row> dataset = sqlspark.read().option("header", true).csv("src/main/resources/data/sql/exams/students.csv");


    //agg method will do the cast to integer
    Dataset<Row> results = dataset.groupBy(col("subject"))
                               .agg(max(col("score")).alias("max_score"),
                                    min(col("score")).alias("min_score"),
                                    avg(round(col("score"), 2)).alias("avg_score"));

    results.show();


    sqlspark.close();
  }
}
