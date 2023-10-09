package com.sparkTutorial.part2typedatasets;

import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import java.io.Serializable;
import org.apache.commons.lang3.RandomStringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import static org.apache.spark.sql.functions.*;

public class UsageCostCalculation {

    public static int main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("UsageCostCalculation").getOrCreate();
        // Create an explicit Encoder
        Encoder<Usage> usageEncoder = Encoders.bean(Usage.class);
        Random rand = new Random();
        rand.setSeed(42);
        List<Usage> data = new ArrayList<Usage>();
        // create 1000 instances of Java Usage class
        for (int i = 0; i < 1000; i++) {
            Usage u = new Usage(i, "user-" + RandomStringUtils.randomAlphanumeric(5), rand.nextInt(1000));
            data.add(u);
        }
        // create a dataset of Usage Java typed-data
        Dataset<Usage> dsUsage = spark.createDataset(data, usageEncoder);
        dsUsage.show(10);



        //Exercise: 1
        // Here filter the usage of the value greatger than 900
        // Define a filter Function and order by usage column

        //Exercise 2
        // define an inline MapFunction
        //Consider this example using the higher-order function map(),
        // where our aim is to find out the usage cost for each user whose usage value is over a certain threshold
        // so we can offer those users a special price per minute.
        //If usage is greater than 750 then increase it 15% or else increase it by 50% and return the UsageCost


        return 0;
    }
}
