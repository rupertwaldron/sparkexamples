package org.ruppyrup;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Sum {

    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sparkContext.parallelize(inputData);
        JavaRDD<Double> sqrtsRdd = myRdd.map(Math::sqrt);
        Integer result = myRdd.reduce(Integer::sum);

        // Get serialization exception as Spart will try to serialize this function and send it out to other cpus
        // Use collect() to bring everything together on to this jvm
        sqrtsRdd.collect().forEach(System.out::println);

        System.out.println("Count = " + sqrtsRdd.count());
        System.out.println("Sum = " + result);


        // count without using count

        JavaRDD<Long> singleValueRdd = sqrtsRdd.map(value -> 1L);
        Long reduce = singleValueRdd.reduce(Long::sum);
        System.out.println("Reduce Count = " + reduce);
        sparkContext.close();
    }
}
