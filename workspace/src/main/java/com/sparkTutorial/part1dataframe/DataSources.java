package com.sparkTutorial.part1dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataSources {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("DataSources")
                .master("local[1]").getOrCreate();


        StructType carsSchema = new StructType(new StructField[]{
                new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Miles_per_Gallon", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Cylinders", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Displacement", DataTypes.DoubleType,true, Metadata.empty()),
                new StructField("Horsepower", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Weight_in_lbs", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Acceleration", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Year", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Origin", DataTypes.StringType, true, Metadata.empty())
        });

     /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
   */
        Dataset<Row> carsDF = session.read()
                .format("json")
                .schema(carsSchema) //enforce a schema
                .option("mode", "failFast") // dropMalformed, permissive (default)
                .option("path", "src/main/resources/data/cars.json")
                .load();

        /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
  */
        carsDF.write()
                .format("json")
                .mode(SaveMode.Overwrite)
                .save("src/main/resources/data/output/cars_dupe.json");

        // JSON flags
        session.read()
                .schema(carsSchema)
                .option("dateFormat", "YYYY-MM-dd") // couple with schema; if Spark fails parsing,
                                                    // it will put null
                .option("allowSingleQuotes", "true")
                .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
                .json("src/main/resources/data/cars.json");


        // CSV flags

        StructType stocksSchema = new StructType(new StructField[]{
                new StructField("symbol", DataTypes.StringType, true, Metadata.empty()),
                new StructField("date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
        });

        session.read()
                .schema(stocksSchema)
                .option("dateFormat", "MMM dd YYYY")
                .option("header", "true")
                .option("sep", ",")
                .option("nullValue", "")
                .csv("src/main/resources/data/stocks.csv");

        // Parquet
        carsDF.write()
                .mode(SaveMode.Overwrite)
                .save("src/main/resources/data/cars");

        /**
         * Exercise: read the movies DF, then write it as
         * - tab-separated values file
         * - snappy Parquet
         * - table "public.movies" in the Postgres DB
         */



    }
}
