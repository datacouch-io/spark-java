package com.sparkTutorial.part3foundations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.broadcast;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class BroadcastJoins {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("Different Spark APIs")
                .master("local[1]").getOrCreate();


        // auto-broadcast detection
        Dataset<Long> bigTable = session.range(1, 100000000);
        Dataset<Long> anotherLargeTable = session.range(1, 100000000); // size estimated by Spark - auto-broadcast
        Dataset<Row> joinedNumbers = anotherLargeTable
                .join(bigTable, "id");

        // deactivate auto-broadcast
        session.conf().set("spark.sql.autoBroadcastJoinThreshold", 1024*10*10*2);

        joinedNumbers.explain();
        joinedNumbers.show(100000);

        Thread.sleep(1000000);
    }
    }
