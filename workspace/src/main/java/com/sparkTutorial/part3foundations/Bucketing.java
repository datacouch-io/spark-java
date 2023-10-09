package com.sparkTutorial.part3foundations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc_nulls_last;

public class Bucketing {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("Bucketing")
                .master("local[1]").getOrCreate();

        session.conf().set("spark.sql.autoBroadcastJoinThreshold", -1);//only to simulate


        Dataset<Row> large = session.range(1000000).selectExpr("id * 5 as id")
                .repartition(10);
        Dataset<Row>  largefile2 = session.range(1000000).selectExpr("id * 3 as id")
                .repartition(3);


        Dataset<Row>  joined = large.join(largefile2, "id");
        joined.explain();
        //joined.show(100000000);

        // bucketing
        large.write()
                .bucketBy(4, "id")//shuffling
                .sortBy("id")//sorting
                .mode("overwrite")//saving into hive tables
                .saveAsTable("bucketed_large");

        largefile2.write()
                .bucketBy(4, "id")
                .sortBy("id")
                .mode("overwrite")
                .saveAsTable("bucketed_small") ;
        // bucketing and saving almost as expensive as a regular shuffle


        session.sql("CACHE table bucketed_large");
        session.sql("CACHE table bucketed_small");

        session.sql("use default");
        Dataset<Row> bucketedLarge = session.table("bucketed_large");
        Dataset<Row> bucketedSmall = session.table("bucketed_small");


        Dataset<Row> bucketedJoin = bucketedLarge.join(bucketedSmall, "id");

        bucketedJoin.explain();
        bucketedJoin.show();




        /**
         * Bucket pruning
         *//*
        Dataset<Row> the10 = bucketedLarge.filter(col("id").equalTo(10));
        the10.show();
        the10.explain();

         joined.count(); // 4-5s
         bucketedJoin.count(); // 4s for bucketing + 0.5s for counting
        mostDelayed.show(); // ~1s
        mostDelayed2.show(); // ~0.2s = 5x perf!*/

Thread.sleep(100000);
    }
    }
