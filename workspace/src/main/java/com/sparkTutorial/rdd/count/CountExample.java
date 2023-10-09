package com.sparkTutorial.rdd.count;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CountExample {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airports");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputFile = sc.textFile("hdfs:///user/training/InputData/word_count.text");
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

        JavaPairRDD countData= wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);


        JavaPairRDD<Object, Object> objectObjectJavaPairRDD =
                wordsFromFile.mapToPair(t -> new Tuple2(t, 1));
        for (Tuple2<Object, Object> test : objectObjectJavaPairRDD.take(10)) //or pairRdd.collect()
        {
            System.out.println(test._1);
            System.out.println(test._2);
        }

        countData.saveAsTextFile("CountData");


    }
}
