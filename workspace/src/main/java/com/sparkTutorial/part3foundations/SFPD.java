package com.sparkTutorial.part3foundations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by cloudera on 1/14/21.
 */
public class SFPD {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("SFPDSolution")
                .config("spark.sql.shuffle.partitions", 4)
                . master("local[1]").getOrCreate();

        Dataset<Row> sfpd = session.read().format("csv").option("inferSchema", "true").load("/Users/neha/Desktop/Neha/Spark/Data/sfpd.numbers")
                .toDF("incidentNum","category","description","dayofweek","date","time","pddistrict","resolution","address","X","Y","pdid");

        sfpd.createOrReplaceTempView("sfpd");

        //session.sql("select * from sfpd").show();

        Dataset<Row> category = sfpd.groupBy("category").count();
        category.show();

        Thread.sleep(10000000);

    }
}











































































































































































