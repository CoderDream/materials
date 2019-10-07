package com.qf.gp1922.day06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JavaSparkWC {
    public static void main(String[] args) {
        // 初始化环境
        final SparkConf conf = new SparkConf();
        conf.setAppName("javasparkwc");
        conf.setMaster("local[2]");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // 获取数据
        final JavaRDD<String> lines = jsc.textFile("c://data/file.txt");

        // 切分数据
        final JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                final List<String> list = Arrays.asList(line.split(" "));
                return list.iterator();
            }
        });

        // 将单词生成一个个元组
        final JavaPairRDD<String, Integer> tup = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        // 聚合
        final JavaPairRDD<String, Integer> sumed = tup.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // Java api并没有提供sortBy，需要将数据的key，value先交换  再排序
        final JavaPairRDD<Integer, String> swaped = sumed.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tup) throws Exception {
//                return new Tuple2<Integer, String>(tup._2, tup._1);
                return tup.swap();
            }
        });

        // 排序
        final JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);

        // 再次交换得到最终的结果
        final JavaPairRDD<String, Integer> res = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tup) throws Exception {
                return tup.swap();
            }
        });

        // 打印
        System.out.println(res.collect());

        jsc.stop();
    }
}
