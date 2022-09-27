package org.ruppyrup.basicrdds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FlatMapRDD {

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

        JavaRDD<String> sentences = sparkContext.parallelize(inputData);

        JavaRDD<String> words = sentences.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        JavaRDD<String> chars = words.filter(word -> word.matches("^[a-zA-Z]+$"));

        chars.collect().forEach(System.out::println);

        sparkContext.close();
    }
}
