package com.sparkTutorial.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    JavaRDD<String> inputFile = sparkContext.textFile("in/word_count.text");

    JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

    JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        countData.collect();
        sparkContext.stop();
}
}






