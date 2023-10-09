package com.sparkTutorial.SQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

public class DepartureDelays {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder()
                .appName("DepartureDelays")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions",5)
                .getOrCreate();

     /*   We will here work on the Airline On-Time
        Performance and Causes of Flight Delays data set,
        which contains data on US flights including date, delay, distance,
        origin, and destination. It’s available as a CSV file with over a million records.
                Using a schema, we’ll read the data into a DataFrame and register the DataFrame
        as a temporary view (more on temporary views shortly) so we can query it with SQL.*/


        Dataset<Row> flightsDF = session.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header","true")
                .load("/<location>/departuredelays.csv");







        //1. show all the longest flights between HNL and JFK--->
        // distance > 1000


        flightsDF.createOrReplaceTempView("us_delay_flights");
        Dataset<Row> gt1000 = session.sql("select distance,origin, " +
                "destination " +
                "from us_delay_flights where distance > 1000 " +
                "ORDER by distance DESC");





        //2. find all the flights between SFO and ORD with at
        // least two hour delay
        //3. if delay > 360 then 'Very Long Delays'
        //if delay >=120 delay <=360, long delays
        //if delay >60 and delay <120, short delays
        //if delay= 0 then 'no delays'
        //id delay = -1 then earliest

        gt1000.show(10);//only for POC
        gt1000.write().format("json").mode(SaveMode.Overwrite)
                .save("out/myflights1");



        gt1000.write()
                .saveAsTable("hiveDeptDelays");




        //1. show all the longest flights between
        // HNL and JFK---> distance > 1000
        //2. find all the flights between SFO
        // and ORD with at least two hour delay

        //3. if delay > 360 then 'Very Long Delays'
        //if delay >=120 delay <=360, long delays
        //if delay >60 and delay <120, short delays
        //if delay= 0 then 'no delays'

//*flightsDF.createGlobalTempView("us_delay_flights_new");

        //the  global table remains accessible as long as the application is alive
        session.newSession().sql("select * from global_temp.us_delay_flights_new");

//exercise 2
        Dataset<Row> myDF = session.sql("select date, delay, origin, destination " +
                "from us_delay_flights " +
                "where delay >= 120 and ORIGIN= 'SFO' and DESTINATION='ORD' " +
                "order by delay DESC");

        myDF.explain();
        //myDF.show();


        Dataset<Row> myDS = session.sql("SELECT delay, origin, destination, " +
                " CASE " +
                " WHEN delay > 360 THEN 'Very Long Delays' " +
                " WHEN delay > 120 AND delay < 360 THEN 'Long Delays' " +
                "WHEN delay > 60 AND delay < 120 THEN 'Short Delays' " +
                "WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays' " +
                "WHEN delay = 0 THEN 'No Delays' " +
                " ELSE 'Early' " +
                " END AS Flight_Delays " +
                " FROM us_delay_flights " +
                " ORDER BY origin, delay DESC");



       //gt1000.cache().show();
        gt1000.persist(StorageLevel.MEMORY_AND_DISK());

        gt1000.unpersist();
        //System.out.println(gt1000.rdd().getNumPartitions());
        //System.out.println(gt1000.rdd().glom());

        //gt1000.show(2000000);
        //the output will go to console--driver(single node)

        gt1000.write().format("parquet").mode(SaveMode.Append).option("delimeter","$")
                .save("/Users/neha/Desktop/Neha/Spark/SparkCode/workspace/out/delays");
        //saved in DFS









        //When i run the job day, i got to create the output folder with 100 part files
        //Day 2 i get some more data 10 more part files,
        // same business logic run the job what will happen to my output folder or the files?

        //dont call repartition
        //hdfs dfs -getMerge -nl /* /destination
        //when i invoke this code, will it create a folder or a file?

    }
}
