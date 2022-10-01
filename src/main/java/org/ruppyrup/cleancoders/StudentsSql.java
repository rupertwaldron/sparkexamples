package org.ruppyrup.cleancoders;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.stddev;


/**
 * This class is used in the chapter late in the course where we analyse viewing figures. You can ignore until then.
 */
public class StudentsSql {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    SparkSession sqlspark = SparkSession.builder()
                                .appName("sqlspark")
                                .master("local[*]")
                                .getOrCreate();

    Dataset<Row> dataset = sqlspark.read().option("header", true).csv("src/main/resources/data/sql/exams/students.csv");

    dataset.createOrReplaceTempView("my_students_table");

    Dataset<Row> sqlresults = sqlspark.sql("select subject, avg(score) as average_score " +
                                            "from my_students_table group by subject order by average_score desc");

    sqlresults.show();

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

    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();

    sqlspark.close();
  }
}
