package com.sparkTutorial.part1dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static java.util.stream.IntStream.concat;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.regexp_replace;

public class ComplexTypes {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("Complex Data Types")
                .master("local[1]").getOrCreate();
        session.conf().set("spark.sql.legacy.timeParserPolicy", "LEGACY");
        Dataset<Row> moviesDF = session.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/movies.json");

        //moviesDF.printSchema();
        // Dates

        Dataset<Row> moviesWithReleaseDates = moviesDF
                .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy")
                        .as("Actual_Release"));// conversion

        Dataset<Row> moviesWithReleaseDateschanged = moviesWithReleaseDates
                .withColumn("Today", current_date()) // today
                .withColumn("Right_Now", current_timestamp()) // this second
                .withColumn("Movie_Age", datediff(col("Today"),
                        col("Actual_Release")).divide(365)); // date_add, date_sub

        moviesWithReleaseDateschanged.printSchema();

        /*moviesWithReleaseDates.select("*").where(col("Actual_Release"))
                .show(5);*/

        /**
         * Exercise
         * 1. How do we deal with multiple date formats?
         * 2. Read the stocks DF and parse the dates
         */

        Dataset<Row> stocksDF = session.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/main/resources/data/stocks.csv");

        //stocksDF.show(5);

        stocksDF.select(
                col("date"),
                to_date(col("date"), "MMM dd yyyy").as("to_date")
        ).withColumn("formatted", date_format(col("to_date"),
                "MM/dd/yyyy")).show(5);


    }
}
