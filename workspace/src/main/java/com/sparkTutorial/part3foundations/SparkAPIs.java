package com.sparkTutorial.part3foundations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class SparkAPIs {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("Different Spark APIs")
                .master("local[1]").getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(session.sparkContext());

        // small count comparison
        List<Integer> integers = makeSequence(10000000);

        JavaRDD<Integer> rdd = sc.parallelize(integers);
        rdd.count();

        session.range(1, 10).toDF("value").createOrReplaceTempView("df");

        Dataset<Row> df = session.range(1, 10).toDF("id");
        df.count(); // ~16s - might vary
        Dataset<Row> dfCount = df.selectExpr("count(*)"); // same
        // look at the Spark UI - there's a wholestagecodegen step in the stage - that's Spark generating the appropriate bytecode to process RDDs behind the scenes
        // most of the time taken is just the RDD transformation - look at the time taken in stage 1


        Dataset<Long> ds = session.range(1, 1000000000);
        ds.count(); // instant, 0.1s
        Dataset<Row> dsCount = ds.selectExpr("count(*)");
        dsCount.show(); // same
        ds.toDF("value").count(); // same

        ds.rdd().count(); // ~25s
        // cmd-click on the `rdd` implementation to see why this is so slow.


    /**
     * Notice that inside the same "realm", i.e. RDDs or DFs, the computation time is small.
     * Converting between them takes a long time.
     * That's because each row is processed individually.
     * Conversions are particularly bad in Python, because the data needs to go from the Python interpreter to the JVM AND back.
     *
     * Lesson 1: once decided on the API level, STAY THERE.
     */

        JavaRDD<Integer> rddTimes5 = rdd.map(x -> x * 5);
        rddTimes5.count(); // ~20s
        // one stage

        Dataset<Row> dfTimes5 = df.select("id * 5 as id");
        Dataset<Row> dfTimes5Count = dfTimes5.selectExpr("count(*)");
        dfTimes5Count.show(); // still 11-12s

        /*
    Notice there's no difference in the time taken, comparing with the original count.
    The RDD version multiplied every single row, but here, the multiplication is instant.
    Or is it?

    WHY?

    scala> dfTimes5Count.explain
    == Physical Plan ==
    *(2) HashAggregate(keys=[], functions=[count(1)])
    +- Exchange SinglePartition
       +- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
          +- *(1) Project
             +- *(1) SerializeFromObject [input[0, int, false] AS value#2]
                +- Scan[obj#1]

    scala> dfCount.explain
    == Physical Plan ==
    *(2) HashAggregate(keys=[], functions=[count(1)])
    +- Exchange SinglePartition
       +- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
          +- *(1) Project
             +- *(1) SerializeFromObject [input[0, int, false] AS value#2]
                +- Scan[obj#1]

    Same query plan! Spark removed the select altogether.
   */
    /**
     * Exercise: measure the time it takes to count the number of elements from the DS, multiplied by 5.
     * Try to explain the difference. It's ok if you have like an 80% explanation.
     */

    }

    static List<Integer> makeSequence(int end) {
        List<Integer> ret = new ArrayList(end - 1 + 1);

        for(int i = 1; i <= end; i++, ret.add(i));

        return ret;
    }
    }
