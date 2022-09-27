package org.ruppyrup.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.stddev;


/**
 * This class is used in the chapter late in the course where we analyse viewing figures. You can ignore until then.
 */
public class StudentsExercise {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    SparkSession sqlspark = SparkSession.builder()
                                .appName("sqlspark")
                                .master("local[*]")
                                .getOrCreate();

    Dataset<Row> dataset = sqlspark.read().option("header", true).csv("src/main/resources/data/sql/exams/students.csv");

//    dataset.show();

    Dataset<Row> results = dataset.select(
            col("subject"),
            col("year"),
            col("score")
        )
                               .groupBy(col("subject"))
                               .pivot(col("year"))
                               .agg(
                                   round(avg(col("score")), 2).alias("avg"),
                                   round(stddev(col("score")), 2).alias("stddev")
                                   );



    results.show();

    Dataset<Row> math = results.select(col("2005_avg"), col("subject")).filter(col("subject").equalTo("Math"));

    math.show();

    Row collect = math.first();

    Double o = collect.getAs(0);

    System.out.println(o);

    sqlspark.close();
  }
}
