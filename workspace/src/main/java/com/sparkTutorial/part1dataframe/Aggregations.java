package com.sparkTutorial.part1dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.IntegerType;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.stddev;
import static org.apache.spark.sql.functions.mean;

public class Aggregations {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("Aggregations and Grouping")
                .master("local[1]").getOrCreate();

        Dataset<Row> moviesDF = session.read()
                .option("inferSchema", "true").option("header","true")
                .format("json")
                .load("src/main/resources/data/movies.json");

        long count = moviesDF.count();
        System.out.println("my count--"+count);
        // counting
        Dataset<Row> genresCountDF = moviesDF.select(count(col("Major_Genre")));// all the values except null
        moviesDF.selectExpr("count(Major_Genre)");

        //final long count = moviesDF.count();

        // counting all
        moviesDF.select(count("*"));// count all the rows, and will INCLUDE nulls


        // counting distinct
        moviesDF.select(countDistinct(col("Major_Genre"))).show();

        // min and max
        Dataset<Row> minRatingDF = moviesDF.select(min(col("IMDB_Rating")));
        moviesDF.selectExpr("min(IMDB_Rating)");

        // sum
        moviesDF.select(sum(col("US_Gross")));
        moviesDF.selectExpr("sum(US_Gross)");

        // avg
        moviesDF.select(
                avg(col("Rotten_Tomatoes_Rating"))
        );
        moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)");

        // data science
        moviesDF.select(
                mean(col("Rotten_Tomatoes_Rating")).cast("integer"),
                stddev(col("Rotten_Tomatoes_Rating"))
        );

        // Grouping
        Dataset<Row> countByGenreDF = moviesDF
                .groupBy(col("Major_Genre")) // includes null
                .count() ; // select count(*) from moviesDF group by Major_Genre

        Dataset<Row> avgRatingByGenreDF = moviesDF
                .groupBy(col("Major_Genre"))
                .avg("IMDB_Rating").as("AVG_IMDB");

        Dataset<Row> aggregationsByGenreDF = moviesDF
                .groupBy(col("Major_Genre"))
                .agg(
                        count("*").as("N_Movies"),
                        avg("IMDB_Rating").as("Avg_Rating")
                )
                .orderBy(col("Avg_Rating"));


        /**
         * Exercises
         *
         * 1. Sum up ALL the profits of ALL the movies in the DF
         *                   // US_Gross , Worldwide_Gross and US_DVD_Sales--> totalGross.sum
         * 2. Count how many distinct directors we have
         * 3. Show the mean and standard deviation of US gross revenue for the movies
         * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
         */


        moviesDF.na().fill(0)
                .selectExpr("Title","US_Gross"
                        ,"Worldwide_Gross","US_DVD_Sales",
                        "US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross")
        .select(sum("Total_Gross")).show()
        ;



        moviesDF.select(
                mean(col("US_Gross")),
                stddev(col("US_Gross"))
        );

        moviesDF.groupBy("Director")
                .agg(
                     avg("IMDB_Rating").as("Avg_Rating"),
                        avg("US_Gross").as("US_Gross_Rating")
                )/*.orderBy(col("Avg_Rating").desc())*/.show();

                Thread.sleep(1000000);
    }
}
