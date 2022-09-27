package org.ruppyrup.basicrdds;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PairRDDs {

    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> originalMessages = sparkContext.parallelize(inputData);

//        JavaPairRDD<String, String> pairRdd = originalMessages.mapToPair(value -> {
//            String[] columns = value.split(":");
//            String level = columns[0];
//            String date = columns[1];
//            return new Tuple2<>(level, date);
//        });

        // Don't use group by key
//        JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = pairRdd.groupByKey();
//
//        stringIterableJavaPairRDD.foreach(value -> {
//            System.out.println(value._1 + "----------");
//            value._2.forEach(System.out::println);
//        });


        JavaPairRDD<String, Long> pairRdd = originalMessages.mapToPair(value -> {
            String[] columns = value.split(":");
            String level = columns[0];
            String date = columns[1];
            return new Tuple2<>(level, 1L);
        });

        System.out.println("After a narrow transformation we have " + pairRdd.getNumPartitions() + " partitions.");

        JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey(Long::sum);
//        JavaPairRDD<String, Iterable<Long>> sumsRdd = pairRdd.groupByKey();

        sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2));

        System.out.println("After a wide transformation we have " + pairRdd.getNumPartitions() + " partitions.");

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();


        sparkContext.close();
    }
}
