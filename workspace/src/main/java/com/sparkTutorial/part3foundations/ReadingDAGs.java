package com.sparkTutorial.part3foundations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadingDAGs {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("Reading query plans")
                .master("local[1]").getOrCreate();

   /*     JavaSparkContext sc = new JavaSparkContext(session.sparkContext());

        ////////////////////////////////////////////////// Boilerplate

        List<Integer> integers = makeSequence(10000000);

        // DAG with a single "box" - the creation of the RDD

        JavaRDD<Integer> rdd1 = sc.parallelize(integers);

        // job 2
        rdd1.map(x->x*2).count();*/

        // DAG with one stage and two "boxes": one for creating the RDD and one for the map
        //rdd1.repartition(23).count();
        // DAG with two stages:
        // stage 1 - the creation of the RDD + exchange
        // stage 2 - computation of the count
        // job 4 - same as query plans:

        Dataset<Long> ds1 = session.range(1, 10000000);
        Dataset<Long> ds2 = session.range(1, 20000000, 2);
        Dataset<Long> ds3 = ds1.repartition(7);
        Dataset<Long> ds4 = ds2.repartition(9);
        Dataset<Row> ds5 = ds3.selectExpr("id * 3 as id");
        Dataset<Row> joined = ds5.join(ds4, "id");
        Dataset<Row> sum = joined.selectExpr("sum(id)");
        // complex DAG

        /**
         * Takeaway: the DAG is a visual representation of the steps Spark will perform to run a job.
         * It's the "drawing" version of the physical query plan.
         * Unlike query plans, which are only available for DataFrames/Spark SQL, DAGs show up for ANY job.
         */

    }

    static List<Integer> makeSequence(int end) {
        List<Integer> ret = new ArrayList(end - 1 + 1);

        for(int i = 1; i <= end; i++, ret.add(i));

        return ret;
    }
    }
