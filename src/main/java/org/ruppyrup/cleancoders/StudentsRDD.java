package org.ruppyrup.cleancoders;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.ruppyrup.basicrdds.BoringUtilities;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Scanner;

public class StudentsRDD {

  public static void main(String[] args) {

    // will not load in full-file, just add partitions
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    JavaRDD<String> inputsRdd = sparkContext.textFile("src/main/resources/data/sql/exams/students.csv");

    String header = inputsRdd.first();

    JavaPairRDD<String, Long> countBySubject = inputsRdd
                                                   .filter(input -> !input.equals(header))
                                                   .map(Result::new)
                                                   .mapToPair((result -> new Tuple2<>(result.subject, 1L)))
                                                   .reduceByKey(Long::sum);

    JavaPairRDD<String, Integer> sumOfScoreBySubject = inputsRdd
                                                           .filter(input -> !input.equals(header))
                                                           .map(Result::new)
                                                           .mapToPair(result -> new Tuple2<>(result.subject, result.score))
                                                           .reduceByKey(Integer::sum);

    JavaPairRDD<String, Tuple2<Integer, Long>> join = sumOfScoreBySubject.join(countBySubject);

    System.out.println("Subject :: Average Score");

    join
        .mapToPair(pair -> new Tuple2<>(pair._1, (double) pair._2._1 / pair._2._2))
        .mapToPair(Tuple2::swap)
        .sortByKey(false)
        .take(30)
        .forEach(pair -> System.out.println(pair._2 + " :: " + pair._1));


    System.out.println("There are " + inputsRdd.getNumPartitions() + " partitions");

    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();

    sparkContext.close();
  }


  public static int average(int a, int b) {
    return (int) (a + b / 2.0);
  }


}


class Result implements Serializable {
  public int id;
  public String subject;

  public int year;
  public int score;
  public String grade;

  public Result(String row) {
    String[] data = row.split(",");
    this.id = Integer.parseInt(data[0]);
    this.subject = data[2];
    this.year = Integer.parseInt(data[3]);
    this.score = Integer.parseInt(data[5]);
    this.grade = data[6];
  }

  @Override
  public String toString() {
    return "Result{" +
               "id=" + id +
               ", subject='" + subject + '\'' +
               ", year=" + year +
               ", score=" + score +
               ", grade='" + grade + '\'' +
               '}';
  }
}
