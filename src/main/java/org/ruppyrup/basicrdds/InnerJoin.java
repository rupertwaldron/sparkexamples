package org.ruppyrup.basicrdds;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

public class InnerJoin {

    public static void main(String[] args) {

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> visits = sparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sparkContext.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> join = visits.join(users);

        join.take(5).forEach(System.out::println);

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoin = visits.leftOuterJoin(users);

        leftOuterJoin.take(5).forEach(it -> System.out.println(it._2._2.or("Missing").toUpperCase()));

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoin = visits.rightOuterJoin(users);

        rightOuterJoin.take(6).forEach(it -> System.out.println("User " + it._2._2 + " had " + it._2._1.or(0) + " visits."));

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianJoin = visits.cartesian(users);

        cartesianJoin.take(100).forEach(System.out::println);

        sparkContext.close();
    }
}
