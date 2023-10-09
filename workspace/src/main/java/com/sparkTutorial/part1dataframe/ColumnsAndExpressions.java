package com.sparkTutorial.part1dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

public class ColumnsAndExpressions {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("DF Columns and Expressions")
                .master("local[1]")
                //.config("spark.sql.shuffle.partitions",100)
                //.enableHiveSupport()
                .getOrCreate();

        Dataset<Row> carsDF = session.read()
                .option("inferSchema", "true")

                .json("src/main/resources/data/cars.json");


        Dataset<Row> americanPowerfulCarsDF3 = carsDF.
                filter("Origin = 'USA' and Name > 150");

        carsDF.select("Names");

        /*carsWithSelectExprWeightsDF.show();


        carsDF.select(
                carsDF.col("Name"),
                col("Acceleration"),
                col("Weight_in_lbs"),
                col("Year"),
                col("Horsepower"),
                expr("Origin") // EXPRESSION
        );

        // select with plain column names
        carsDF.select("Name", "Year");

        // EXPRESSIONS
        Column simplestExpression = carsDF.col("Weight_in_lbs");
        Column weightInKgExpression = carsDF.col("Weight_in_lbs").divide( 2.2);

        Dataset<Row> carsWithWeightsDF = carsDF.select(
                col("Name"),
                col("Weight_in_lbs"),
                weightInKgExpression.as("Weight_in_kg"),
                expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
        );


        //carsWithWeightsDF.show(5);

        // selectExpr
        Dataset<Row>  carsWithSelectExprWeightsDF1 = carsDF.selectExpr(
                "Name",
                "Weight_in_lbs",
                "Weight_in_lbs / 2.2"
        );

        // DF processing

        // adding a column
        Dataset<Row>  carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3",
                col("Weight_in_lbs").divide(2.2));

        // renaming a column
        Dataset<Row>  carsWithColumnRenamed = carsDF
                .withColumnRenamed("Weight_in_lbs", "Weight in pounds");

        // careful with column names
        carsWithColumnRenamed.selectExpr("`Weight in pounds`");
        // remove a column
        carsWithColumnRenamed.drop("Cylinders", "Displacement");

        // filtering
        Dataset<Row> europeanCarsDF = carsDF.filter(col("Origin").notEqual("USA"));
        Dataset<Row> europeanCarsDF2 = carsDF.where(col("Origin").notEqual("USA"));

        // filtering with expression strings
        Dataset<Row> americanCarsDF = carsDF.filter("Origin = 'USA'");

        // chain filters
        carsDF.filter((carsDF.col("Origin").contains("USA") )
                .and(carsDF.col("Horsepower").gt(150)));



        Dataset<Row> americanPowerfulCarsDF3 = carsDF.
                filter("Origin = 'USA' and Horsepower > 150");

        // unioning = adding more rows
        Dataset<Row> moreCarsDF = session.read().option("inferSchema", "true")
                .json("src/main/resources/data/more_cars.json");

        Dataset<Row> allCarsDF = carsDF.union(moreCarsDF); // works if the DFs have the same schema

        // distinct values
        Dataset<Row> allCountriesDF = carsDF.select("Origin").distinct();

        *//**
         * Exercises
         *
         * 1. Read the movies DF and select 2 columns of your choice
         * 2. Create another column summing up the total profit of the movies
         *                           = US_Gross + Worldwide_Gross + DVD sales
         * 3. Select all COMEDY movies with IMDB rating above 6
         *
         * Use as many versions as possible
         *//*
        //allCountriesDF.write().format("json").mode(SaveMode.Overwrite).save("out/myfolder");
*/
    }
}
