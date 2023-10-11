package com.sparkTutorial.part2typedatasets;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import java.util.Arrays;

public class UDFExample {

    public static void main(String[] args) {
        // Create a SparkSession
        SparkSession sparkSession = SparkSession.builder()
                .appName("UDFExample")
                .master("local[*]")
                .getOrCreate();

        // Create a DataFrame
        Dataset<Row> tempDF = sparkSession.createDataFrame(Arrays.asList(
                RowFactory.create("Rahul Sharma", 32, "Patna", 20000, "Store Manager"),
                RowFactory.create("Joy Don", 30, "NY", 23455, "Developer"),
                RowFactory.create("Steve Boy", 42, "Delhi", 294884, "Developer")
        ), DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("salary", DataTypes.IntegerType, true),
                DataTypes.createStructField("designation", DataTypes.StringType, true)
        )));

        // Step 2: Create a UDF to get the first name
        UserDefinedFunction getFirstNameUDF = functions.udf((String name) -> {
            String[] temp = name.split(" ");
            return temp[0];
        }, DataTypes.StringType);

        // Register the UDF
        sparkSession.udf().register("getFirstName", getFirstNameUDF);

        // Step 3: Create another UDF to check if the person is a manager
        UserDefinedFunction isManagerUDF = functions.udf((String designation) -> {
            if (designation.contains("Manager"))
                return "Yes";
            else
                return "No";
        }, DataTypes.StringType);

        // Register the UDF
        sparkSession.udf().register("isManager", isManagerUDF);

        // Step 4: Use the UDFs with the DataFrame
        Dataset<Row> finalDF = tempDF.withColumn("first name", functions.callUDF("getFirstName", functions.col("name")))
                .withColumn("is_manager", functions.callUDF("isManager", functions.col("designation")));

        finalDF.show();
        sparkSession.stop();
    }
}
