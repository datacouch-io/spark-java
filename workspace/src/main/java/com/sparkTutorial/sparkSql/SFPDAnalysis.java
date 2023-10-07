package com.sparkTutorial.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Neha Priya on 04-10-2019.
 */
public class SFPDAnalysis {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("SFPDAnalysis").
                        master("local[1]").getOrCreate();

        JavaRDD<SFPD> peopleRDD = spark.read()
                .textFile("C:\\Neha\\personal\\CTS\\Data\\Datasets\\crime_incidents.csv")
                .javaRDD()
                .map(new Function<String, SFPD>() {
                    @Override
                    public SFPD call(String line) throws Exception {
                        String[] parts = line.split("([^\"]*)\"|(?<=,|^)([^,]*)(?=,|$)");
                        SFPD person = new SFPD();

                        person.setIncidntNum(parts[0]);
                        person.setCategory(parts[1]);
                        person.setDescription(parts[2]);
                        person.setDayOfWeek(parts[3]);
                        person.setDateOfIncident(parts[4]);
                        person.setTimeOfIncident(parts[5]);
                        person.setPdDistrict(parts[6]);
                        person.setResolution(parts[7]);
                        person.setAddress(parts[8]);
                        person.setX(parts[9]);
                        person.setY(parts[10]);
                        person.setLocation(parts[11]);
                        return person;
                    }
                });

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, SFPD.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("sfpd");
        peopleDF.printSchema();
    }
}
