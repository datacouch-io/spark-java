package com.sparkTutorial.part3foundations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PrePartitioning {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("Pre-Partitioning")
                .master("local[1]").getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // deactivate broadcast joins
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", 1024*12);

  /*
    addColumns(initialTable, 3) => dataframe with columns "id", "newCol1", "newCol2", "newCol3"
   */

        // don't touch this
        Dataset<Long> initialTable = spark.range(1, 10000000).repartition(10); // RoundRobinPartitioning(10)
        Dataset<Long> narrowTable = spark.range(1, 5000000).repartition(7) ;// RoundRobinPartitioning(7)

       /* // scenario 1
        Dataset<Long> wideTable = addColumns(initialTable, 30);
        Dataset<Row> join1 = wideTable.join(narrowTable, "id");
        join1.explain();*/
        // println(join1.count()) // around 20s
    }


}
