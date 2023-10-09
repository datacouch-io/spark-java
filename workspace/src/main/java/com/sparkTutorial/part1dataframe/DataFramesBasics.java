package com.sparkTutorial.part1dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;


public class DataFramesBasics {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("DataFramesBasics")
               .master("local[*]").getOrCreate();

        Dataset<Row> firstDF = session.read()
                .format("json")
                .option("inferSchema", "true")
                .load("src/main/resources/data/cars.json");

        //firstDF.show();
        //firstDF.printSchema();
        //System.out.println(firstDF.schema());


        StructType carsSchema = new StructType(new StructField[]{
                new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Miles_per_Gallon", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Cylinders", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Displacement", DataTypes.DoubleType,true, Metadata.empty()),
                new StructField("Horsepower", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Weight_in_lbs", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Acceleration", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Year", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Origin", DataTypes.StringType, true, Metadata.empty())
        });



        // read a DF with your schema
        Dataset<Row> carsDFWithSchema = session.read()
                .format("json")
                .schema(carsSchema)
                .load("src/main/resources/data/cars.json");

        //carsDFWithSchema.select("NME");//there is no action

        //carsDFWithSchema.show(2);

        // create rows by hand
        Row myRow = RowFactory.create("chevrolet chevelle malibu", 18.0, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA");


        List<Row> cars=new ArrayList<Row>();
        cars.add(RowFactory.create("chevrolet chevelle malibu",18.0,8,307.0,130,3504,12.0,"1970-01-01","USA"));
        cars.add(RowFactory.create("buick skylark 320",15.0,8,350.0,165,3693,11.5,"1970-01-01","USA"));
        cars.add(RowFactory.create("plymouth satellite",18.0,8,318.0,150,3436,11.0,"1970-01-01","USA"));
        cars.add(RowFactory.create("amc rebel sst",16.0,8,304.0,150,3433,12.0,"1970-01-01","USA"));
        cars.add(RowFactory.create("ford torino",17.0,8,302.0,140,3449,10.5,"1970-01-01","USA"));
        cars.add(RowFactory.create ("ford galaxie 500",15.0,8,429.0,198,4341,10.0,"1970-01-01","USA"));
        cars.add(RowFactory.create("chevrolet impala",14.0,8,454.0,220,4354,9.0,"1970-01-01","USA"));
        cars.add(RowFactory.create("plymouth fury iii",14.0,8,440.0,215,4312,8.5,"1970-01-01","USA"));
        cars.add(RowFactory.create("pontiac catalina",14.0,8,455.0,225,4425,10.0,"1970-01-01","USA"));
        cars.add(RowFactory.create("amc ambassador dpl",15.0,8,390.0,190,3850,8.5,"1970-01-01","USA"));

        // note: DFs have schemas, rows do not
        Dataset<Row> manualCarsDF = session.createDataFrame(cars,carsSchema);
        //manualCarsDF.show(3);



        /**
         * Exercise:
         * 1) Create a manual DF describing smartphones
         *   - make
         *   - model
         *   - screen dimension
         *   - camera megapixels
         *
         * 2) Read another file from the data/ folder, e.g. movies.json
         *   - print its schema
         *   - count the number of rows, call count()
         */

        List<Row> smartPhones=new ArrayList<Row>();
        smartPhones.add(RowFactory.create("Samsung","Galaxy","Android",12));
















        Dataset<Row> moviesDF = session.read()
                .format("json")
                .option("inferSchema", "true")
                .load("src/main/resources/data/movies.json");


        //Row[] collect = moviesDF.rdd().collect();

        moviesDF.withColumn("Total_Profit",
                col("US_Gross").plus(col("Worldwide_Gross")).plus(col("US_DVD_Sales"))).show();



        moviesDF.na().fill(0)
                .selectExpr("Title","US_Gross"
                        ,"Worldwide_Gross","US_DVD_Sales",
                        "US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross")
                .selectExpr("sum(Total_Gross) as Total_profit");



        moviesDF.select("Title","IMDB_Rating")
                .filter(col("Major_Genre").equalTo("Comedy"))
                .filter(col("IMDB_Rating").gt(6)).show();


















        moviesDF.select(col("IMDB_Rating").cast("int").as("newRating"))
                .where(col("IMDB_Rating").gt(6)).show();

        moviesDF.selectExpr("cast(IMDB_Rating as int) intrating");

        //moviesDF.printSchema();
        //System.out.println("total movies"+moviesDF.count());

    }
}
