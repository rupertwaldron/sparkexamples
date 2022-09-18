package org.ruppyrup;

import java.util.Arrays;
import java.util.Scanner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class ReadingFromFile {

    public static void main(String[] args) {

        // will not load in full-file, just add partitions
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> inputsRdd = sparkContext.textFile("src/main/resources/data/subtitles/input.txt");

        inputsRdd
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(word -> word.matches("^[a-zA-Z]+$"))
                .map(String::toLowerCase)
                .filter(BoringUtilities::isNotBoring)
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
                .sortByKey(false)
                .take(100)
//                .coalesce(1) moves all data onto one partition
//                .collect() // this will put everything on one partition, with big data this will result in out of memory
//                        .forEach(System.out::println);
                .forEach(entry -> System.out.println("Key = " + entry._1 + " :: " + entry._2));
        // foreach executes on 2 partitions 2 threads running foreach in parallel - results are interwoven
        // using a function like take(10) will know the data is separated but will combine in the correct way

        System.out.println("There are " + inputsRdd.getNumPartitions() + " partitions");

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        sparkContext.close();
    }
}
