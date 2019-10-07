package com.qf.gp1922.day06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;

public class LambdaJavaWC {
    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("lambdajavawc").setMaster("local[2]");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<String> lines = jsc.textFile(args[0]);
        final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        final JavaPairRDD<String, Integer> tups = words.mapToPair(word -> new Tuple2<>(word, 1));
        final JavaPairRDD<String, Integer> sumed = tups.reduceByKey((x, y) -> x + y);
        final JavaPairRDD<Integer, String> swaped = sumed.mapToPair(tup -> tup.swap());
        final JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        final JavaPairRDD<String, Integer> res = sorted.mapToPair(tup -> tup.swap());
        System.out.println(res.collect());
        res.saveAsTextFile(args[1]);

        jsc.stop();
    }
}
