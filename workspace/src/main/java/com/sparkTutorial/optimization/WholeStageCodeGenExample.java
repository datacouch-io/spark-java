package com.sparkTutorial.optimization;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Neha Priya on 04-01-2019.
 */
public class WholeStageCodeGenExample {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("HousePriceSolution")
                .config("spark.sql.codegen.wholeStage", "false").
                master("local[1]").getOrCreate();


      //session.range(10000L * 10000 * 10000).selectExpr("sum(id)").show();

        Dataset<Row> ds = session.range(1000L * 1000 * 1000).
                join(session.range(1000L).toDF(), "id").selectExpr("count(*)");

        //ds.rdd().toDebugString();
        ds.explain();
        ds.show();
        Thread.sleep(50000);

    }
}
