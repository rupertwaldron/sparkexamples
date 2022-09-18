package org.ruppyrup.viewingexercise;

import java.util.Scanner;
import java.util.stream.Collectors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class ViewingFigures {

  public static void main(String[] args) {

    Logger.getLogger("org.apache").setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    JavaRDD<String> chaptersAndCourses = sparkContext.textFile(
        "src/main/resources/data/viewingfigures/chapters.csv").cache();

    // courseId : no of chapters
    JavaPairRDD<String, Long> chaptersPerCourse = chaptersAndCourses.mapToPair(
            value -> new Tuple2<>(value.split(",")[1], 1L))
        .reduceByKey(Long::sum);

    // : chapterId : courseId
    JavaPairRDD<String, String> chaptersVsCourses = chaptersAndCourses.mapToPair(value -> {
      String[] split = value.split(",");
      return new Tuple2<>(split[0], split[1]);
    });

    JavaRDD<String> usersAndChapters1 = sparkContext.textFile(
        "src/main/resources/data/viewingfigures/views-1.csv");

    JavaRDD<String> usersAndChapters2 = sparkContext.textFile(
        "src/main/resources/data/viewingfigures/views-2.csv");

    JavaRDD<String> usersAndChapters3 = sparkContext.textFile(
        "src/main/resources/data/viewingfigures/views-3.csv");

    JavaRDD<String> usersAndChapters = usersAndChapters1.union(usersAndChapters2).union(usersAndChapters3);

    // chapterId : UserId
    JavaPairRDD<String, String> viewsPerChapter = usersAndChapters.mapToPair(value -> {
          String[] split = value.split(",");
          return new Tuple2<>(split[1], split[0]);
        })
        .distinct();

    JavaPairRDD<String, Tuple2<String, String>> viewsPerCourse = viewsPerChapter.join(chaptersVsCourses);

    // courseId : views
    JavaPairRDD<Tuple2<String, String>, Long> userIdCourseIdCount = viewsPerCourse
        .mapToPair(value -> new Tuple2<>(value._2, 1L))
        .reduceByKey(Long::sum);

    JavaPairRDD<String, Long> courseIdViewCount = userIdCourseIdCount
        .mapToPair(value -> new Tuple2<>(value._1._2, value._2));

    JavaPairRDD<String, Tuple2<Long, Long>> courseIdvsViewOf = courseIdViewCount.join(chaptersPerCourse);

//    courseIdvsViewOf
//        .take(20)
//        .forEach(System.out::println);

    JavaPairRDD<String, Double> courseIdVsPerc = courseIdvsViewOf.mapToPair(
        value -> new Tuple2<>(value._1, value._2._1 / ((double) value._2._2)));

    JavaPairRDD<String, Integer> courseScores = courseIdVsPerc.mapToPair(value -> {
      int score = 0;
      if (value._2 >= 0.9) {
        score = 10;
      } else if (value._2 >= 0.5 && value._2 < 0.9) {
        score = 4;
      } else if (value._2 >= 0.25 && value._2 < 0.5) {
        score = 2;
      }

      return new Tuple2<>(value._1, score);
    });

    JavaPairRDD<String, Integer> result = courseScores
        .reduceByKey(Integer::sum);

    JavaPairRDD<String, String> courseIdtitles = sparkContext.textFile(
            "src/main/resources/data/viewingfigures/titles.csv")
        .mapToPair(value -> {
          String[] split = value.split(",");
          return new Tuple2<>(split[0], split[1]);
        });

    JavaPairRDD<Integer, String> resultWithTitles = result.join(courseIdtitles)
        .mapToPair(value -> new Tuple2<>(value._2._1, value._2._2))
        .sortByKey(false);

    resultWithTitles.
        take(50)
        .forEach(value -> System.out.println("Title : " + value._2 + " has a score of : " + value._1));

//    long count = resultWithTitles
//        .count();

//    System.out.println("Count = " + count);

    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();


  }


}
