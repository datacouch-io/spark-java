package com.sparkTutorial.SQL;



import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

/**
 * Created by Neha Priya on 6/12/23.
 */
public class JoinsDemo {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder()
                .appName("OrderDemo")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.shuffle.partitions", 5)
                .getOrCreate();

        session.conf().set("spark.sql.autoBroadcastJoinThreshold", "-1");
        //only to stub as to behave we have two large DFs
        //we never do it in production



        Dataset<Row> orderDF = session.read()
                .format("json")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("/user/training/orderData");

        Dataset<Row> userDF = session.read()
                .format("json")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("/user/training/usersData");


        userDF.orderBy(col("uid").asc()).write().format("parquet")
                .bucketBy(5,"uid").mode(SaveMode.Overwrite)
                .saveAsTable("usersOrderedSortedTable");


        orderDF.orderBy(col("users_id").asc()).write().format("parquet")
                .bucketBy(5,"users_id").mode(SaveMode.Overwrite)
                .saveAsTable("orderOrderedSortedTable");

        Dataset<Row> usersOrderedSortedTableDf = session.read().table("usersOrderedSortedTable");

        //Dataset<Row> usersOrderedSortedTableDf1 = session.sql("select * from usersOrderedSortedTable");

        Dataset<Row> orderOrderedSortedTableeDf = session.read().table("orderOrderedSortedTable");

        Column joincondition = orderOrderedSortedTableeDf.col("users_id")
                .equalTo(usersOrderedSortedTableDf.col("uid"));


        Dataset<Row> userAndOrders = orderOrderedSortedTableeDf
                .join(usersOrderedSortedTableDf, joincondition, "inner");

        userAndOrders.show(10);

        Thread.sleep(10000000);

    }
}



//bin/spark-submit --master yarn --class training.day6.JoinsDEmo /home/cloudera/Desktop/SW/Code/workspace/workspace/target/sparkChennai-1.0-SNAPSHOT.jar
