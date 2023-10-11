package com.sparkTutorial;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.RowFactory;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class CBOExample {
    public static void main(String[] args) {
        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("CBOExample")
                .master("local[*]")
                .getOrCreate();

        // Enable Cost-Based Optimization
        spark.conf().set("spark.sql.cbo.enabled", "true");
        spark.conf().set("spark.sql.cbo.joinReorder.enabled", "true");

        // Sample data for orders DataFrame
        Row[] orderData = new Row[]{
                RowFactory.create(1, "2023-01-01", "Alice"),
                RowFactory.create(2, "2023-01-02", "Bob"),
                RowFactory.create(3, "2023-01-03", "Charlie")
        };

        // Sample data for orderDetails DataFrame
        Row[] orderDetailsData = new Row[]{
                RowFactory.create(1, 100.0),
                RowFactory.create(2, 150.0),
                RowFactory.create(1, 75.0),
                RowFactory.create(3, 200.0),
                RowFactory.create(2, 50.0)
        };

        // Define the schema for orders DataFrame
        StructType orderSchema = new StructType()
                .add("order_id", DataTypes.IntegerType, false)
                .add("order_date", DataTypes.StringType, false)
                .add("customer", DataTypes.StringType, false);

        // Define the schema for orderDetails DataFrame
        StructType orderDetailsSchema = new StructType()
                .add("order_id", DataTypes.IntegerType, false)
                .add("amount", DataTypes.DoubleType, false);

        // Create DataFrames from the sample data
        Dataset<Row> orders = spark.createDataFrame(Arrays.asList(orderData), orderSchema);

        // Convert the date strings to DateType using to_date function with explicit date format
        orders = orders.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"));

        Dataset<Row> orderDetails = spark.createDataFrame(Arrays.asList(orderDetailsData), orderDetailsSchema);

        // Perform a join operation to calculate total order amount
        Dataset<Row> joinedDF = orders.join(orderDetails, "order_id")
                .groupBy("order_id")
                .agg(sum("amount").alias("total_amount"));

        // Show the result
        joinedDF.show();

        // Stop the SparkSession
        spark.stop();
    }
}
