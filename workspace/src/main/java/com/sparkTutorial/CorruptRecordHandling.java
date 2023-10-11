package com.sparkTutorial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CorruptRecordHandling {

    static Logger log = Logger.getLogger(CorruptRecordHandling.class);

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws AnalysisException {

        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("CorruptRecordHandling")
                .master("local[*]")
                .getOrCreate();

        // Initialize JavaSparkContext (if needed)
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        String str = "{\"a\": 1, \"b\":2, \"c\":3}@{\"a\": 1, \"b\":2, \"c\":3}@{\"a\": 1, \"b, \"c\":10}";

        // String str = "{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}";
        String[] arrOfStr = str.split("@");

        System.out.println(arrOfStr.length);

        // convert the Array to List

        Dataset<Row> corruptDF = spark.read().option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record").json(sc.parallelize(Arrays.asList(arrOfStr)));

        corruptDF.show();

        String str1 = "{'a': 1, 'b':2, 'c':3}@{'a': 1, 'b':2, 'c':3}@{'a': 1, 'b, 'c':10}";
        String[] arrOfStr1 = str1.split("@");

        Dataset<Row> corruptDF1 = spark.read().option("mode", "DROPMALFORMED")
                .json(sc.parallelize(Arrays.asList(arrOfStr1)));

        corruptDF1.show();

        // ... (more code for handling different modes)

        // Stop SparkSession
        spark.stop();
    }

    public static <T> List<T> convertArrayToList(T array[]) {
        // Create an empty List
        List<T> list = new ArrayList<>();
        // Iterate through the array
        for (T t : array) {
            // Add each element into the list
            list.add(t);
        }
        // Return the converted List
        return list;
    }
}
