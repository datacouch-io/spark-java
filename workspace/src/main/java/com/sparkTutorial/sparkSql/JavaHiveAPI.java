package com.sparkTutorial.sparkSql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaHiveAPI {
    public static void main(String[] args) {

        SparkSession mySparkSession = SparkSession.builder()
                .master("local")
                .appName("Spark-SQL Hive Integration ")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "your path")
                .getOrCreate();

        mySparkSession.sql("CREATE TABLE IF NOT EXISTS "
                +" CDRs (callingNumber STRING, calledNumber String, "
                +"origin String, Dest String,CallDtTm String, callCharge Int) "
                +" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");


        mySparkSession.sql("LOAD DATA LOCAL INPATH '"+"/C://Neha/Datasets/cdrs.csv"+" " +
                "'INTO TABLE CDRs");

                mySparkSession.sql(" SELECT origin, dest, count(*) as cnt "
                        +" FROM CDRs "
                        +" GROUP by origin, dest "
                        +" ORDERR by cnt desc "
                        +"  LIMIT 5").show();

    }
}
