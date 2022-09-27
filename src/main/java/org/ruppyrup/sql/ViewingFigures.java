package org.ruppyrup.sql;

import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


/**
 * This class is used in the chapter late in the course where we analyse viewing figures. You can ignore until then.
 */
public class ViewingFigures {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);
//		SparkConf conf = new SparkConf().setAppName("sqlspark").setMaster("local[*]");
    SparkSession sqlspark = SparkSession.builder()
                                .appName("sqlspark")
                                .master("local[*]")
                                .getOrCreate();

    Dataset<Row> dataset = sqlspark.read().option("header", true).csv("src/main/resources/data/sql/exams/students.csv");
//    dataset.show(); // prints out first 20 rows

//    long count = dataset.count();
//    System.out.println("Count of data = " + count);

    Row firstRow = dataset.first();
    String subject = firstRow.getAs("subject");
    System.out.println("Row one " + subject);

    // approach 1 Dataset<Row> modernArt = dataset.filter("subject = 'Modern Art' and year >= 2007");

//    approach 2Dataset<Row> modernArt = dataset.filter((Function1<Row, Object>) row -> row.getAs("subject").equals("Modern Art"));

//    Column subjectCol = dataset.col("subject");
//    Column yearCol = dataset.col("year");

    Column subjectCol = dataset.col("subject");
    Column yearCol = dataset.col("year");

//    Dataset<Row> modernArt = dataset
//        .filter(subjectCol.equalTo("Modern Art")
//        .and(yearCol.geq(2007)));

    Dataset<Row> modernArt = dataset.filter(col("subject").equalTo("Modern Art")
                                                .and(col("year").geq(2007)));

    modernArt.show();

    dataset.createOrReplaceTempView("my_students_table");

//    Dataset<Row> results = sqlspark.sql("select score, year from my_students_table where subject = 'French'");
//    Dataset<Row> results = sqlspark.sql("select avg(score) from my_students_table where subject = 'French'");
    Dataset<Row> results = sqlspark.sql("select distinct(year) from my_students_table");

    results.show();


    sqlspark.close();
  }
}
