package org.ruppyrup.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.stddev;


/**
 * This class is used in the chapter late in the course where we analyse viewing figures. You can ignore until then.
 */
public class StudentsUserDefined {

  @SuppressWarnings("resource")
  public static void main(String[] args) {

    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org").setLevel(Level.WARN);

    SparkSession sqlspark = SparkSession.builder()
                                .appName("sqlspark")
                                .master("local[*]")
                                .getOrCreate();

    sqlspark.udf().register("hasPassed", (String grade, String subject) -> {

      if (subject.equals("Biology")) {
        return grade.startsWith("A");
      }
      return grade.startsWith("A+") || grade.startsWith("B");
    }, DataTypes.BooleanType);

    Dataset<Row> dataset = sqlspark.read().option("header", true).csv("src/main/resources/data/sql/exams/students.csv");

//    dataset.show();

//    Dataset<Row> results = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));
    Dataset<Row> results = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));



    results.show();

    sqlspark.close();
  }
}
