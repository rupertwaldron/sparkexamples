package org.ruppyrup.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;


/**
 * This class is used in the chapter late in the course where we analyse viewing figures. You can ignore until then.
 */
public class UdfSqlmprovement {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    SparkSession sqlspark = SparkSession.builder()
                                .appName("sqlspark")
                                .master("local[*]")
                                .getOrCreate();

    Dataset<Row> dataset = sqlspark.read().option("header", true).csv("src/main/resources/data/sql/biglog.txt");

//    dataset = dataset.selectExpr("level", "date_format(datetime, 'MMMM') as month");

    dataset.createOrReplaceTempView("logging_table");


    SimpleDateFormat input= new SimpleDateFormat("MMMM");
    SimpleDateFormat output= new SimpleDateFormat("M");

    sqlspark.udf().register("monthNum", (String datetime) -> {
        Date inputDate = input.parse(datetime);
        return Integer.parseInt(output.format(inputDate));
    }, DataTypes.IntegerType);

//    Dataset<Row> results = sqlspark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
//                                        "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");


    Dataset<Row> results = sqlspark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                                        "from logging_table group by level, month order by monthNum(month), level");

//    List<Object> months = List.of("January", "February", "March", "April", "May", "June", "July", "August", "AugSeptember", "September", "October", "November", "December");

//    Dataset<Row> results = dataset.select(
//            col("level"),
//            date_format(col("datetime"), "MMMM").alias("month"),
//            date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType)
//        )
//                               .groupBy(col("level"))
//                               .pivot(col("month"), months)
//                               .count()
//                               .na() // if blank do this
//                               .fill(0); // drop will drop any zero rows

    results.show(100);

    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();

    sqlspark.close();
  }
}
