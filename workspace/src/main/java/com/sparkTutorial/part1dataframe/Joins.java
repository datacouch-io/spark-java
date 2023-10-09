package com.sparkTutorial.part1dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.expr;

public class Joins {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("Joins")
                .master("local[1]").getOrCreate();

        Dataset<Row> guitarsDF = session.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/guitars.json");

        Dataset<Row>  guitaristsDF = session.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/guitarPlayers.json");

        Dataset<Row>  bandsDF = session.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/bands.json");

        // inner joins
        Column joinCondition = guitaristsDF.col("band")
                .equalTo(bandsDF.col("id"));


        Dataset<Row> guitaristsBandsDF = guitaristsDF.join(bandsDF,
                guitaristsDF.col("band").equalTo(bandsDF.col("id")),
                "inner");


        guitaristsBandsDF.show(5);

        guitaristsBandsDF.select("id", "band").show ();//what will be the output?

        //m-1#drop the dupe column
        guitaristsBandsDF.drop(bandsDF.col("id"));

        //renaming the column
        guitaristsDF.join(bandsDF.withColumnRenamed("id","band")
                ,"band");




       // outer joins
        // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
        guitaristsDF.join(bandsDF, joinCondition, "left_outer");

        // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
        guitaristsDF.join(bandsDF, joinCondition, "right_outer");

        // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
        guitaristsDF.join(bandsDF, joinCondition, "outer");

        // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
        guitaristsDF.join(bandsDF, joinCondition, "left_semi");

        // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
        guitaristsDF.join(bandsDF, joinCondition, "left_anti");

        // things to bear in mind


        // option 1 - rename the column on which we are joining
        guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"),
                "band");

        // option 2 - drop the dupe column
        guitaristsBandsDF.drop(bandsDF.col("id"));

        // option 3 - rename the offending column and keep the data
        Dataset<Row> bandsModDF = bandsDF.withColumnRenamed("id", "bandId");
        guitaristsDF.join(bandsModDF, guitaristsDF.col("band").
                equalTo(bandsModDF.col("bandId")));

        // using complex types
        guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId")
                , expr("array_contains(guitars, guitarId)"));

    //Exercise
        /*
        Read the employees.csv and show all the employees and their max salary
        Find the jobs of the best paid 10 employees in the company

         Exercise 2#
         Load the github.json and github-top20.json into dataframes
         join these two DFs
         group by actor.login
         count the results and order by descending count

         */

    }

}
