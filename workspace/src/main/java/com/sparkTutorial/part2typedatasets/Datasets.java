package com.sparkTutorial.part2typedatasets;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import static org.apache.spark.sql.functions.col;


public class Datasets {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("Complex Data Types")
                .master("local[1]").getOrCreate();

        Dataset<Row> numbersDF = session.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("src/main/resources/data/numbers.csv");

        numbersDF.printSchema();

        Encoder<Integer> intEncoder = Encoders.INT();
        Dataset<Integer> numbersDS = numbersDF.as(intEncoder);

        Dataset<Row> carsDF= session.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/cars.json")
                .na().fill(0);

        carsDF.printSchema();


        Encoder<Car> carEncoder = Encoders.bean(Car.class);
        Dataset<Car> carsDS = carsDF.as(carEncoder);

        // DS collection functions

        // In Java
        // Define a Java filter function
        FilterFunction<Integer> f = new FilterFunction<Integer>() {
            public boolean call(Integer u) {
                return (u < 100);
            }
        };

        numbersDS.filter(f);

        // map, flatMap, fold, reduce, for comprehensions ...

        // In Java
        // Define an inline MapFunction
        Dataset<Car> mapCarUpperCase = carsDS
                .map((MapFunction<Car, Car>) u -> {
            return new Car(u.Name.toUpperCase(),
                    u.Miles_per_Gallon, u.Cylinders, u.Displacement,
                    u.Horsepower, u.Weight_in_lbs, u.Acceleration,
                    u.Year, u.Origin);
        }, carEncoder)
                .filter((FilterFunction<Car>)
                        x -> (x.Horsepower > 140));
        //2
        carsDS.filter((FilterFunction<Car>)
                x -> (x.Horsepower > 140));


        carsDS.count();
        mapCarUpperCase.show(5); // We need to explicitly specify the Encoder

        /**
         * Exercises
         *
         * 1. Count how many cars we have
         * 2. Count how many POWERFUL cars we have (HP > 140)
         * 3. Average HP for the entire dataset
         */

        long count = carsDS.count();
        System.out.println(count);

        //3


        carsDS.agg(avg("Horsepower")).show();

        Car reduceHP = carsDS.map((MapFunction<Car, Car>) x ->
        {
            return new Car(x.Horsepower);
        }, carEncoder)
                .reduce((ReduceFunction<Car>) (v1, v2) -> {
                    return new Car(v1.Horsepower
                            + v2.Horsepower);
                });

        long avg = reduceHP.Horsepower/count;


        //ds
        //FilterFunction<Car> fn = ;
        Dataset<Car> filterCarDS = carsDS.filter((FilterFunction<Car>)
                x -> (x.Horsepower > 140));
        //compile time safety
        filterCarDS.show();

        //music.json
        //Title,artist,price,category


        /*
        1. Create a DF
        2. convert the DF to DS
        3. Filter the musicDS get all the items in the "Pop" category(use lambda exp)
        Filter the musicDF get all the items in the "Pop" category
        4.transform the data on musicDS so that the price is reduced to 10%(multiply by 0.9)
        transform the data on musicDF so that the price is reduced to 10%(multiply by 0.9)
        5. using music DS make a mistake intentionally in transforming when you modify
        the price
        try multiplying "A" and observe what happens?
         */


    }


    public static class Car implements Serializable {
        String Name;
        Double Miles_per_Gallon;
        Long Cylinders;
        Double Displacement ;
        Long Horsepower;
        Long Weight_in_lbs;
        Double Acceleration ;
        String Year;
        String Origin;

        public Car() {
        }

        public Car(Long horsepower) {
            Horsepower = horsepower;
        }

        @Override
        public String toString() {
            return "Car{" +
                    "Name='" + Name + '\'' +
                    ", Miles_per_Gallon=" + Miles_per_Gallon +
                    ", Cylinders=" + Cylinders +
                    ", Displacement=" + Displacement +
                    ", Horsepower=" + Horsepower +
                    ", Weight_in_lbs=" + Weight_in_lbs +
                    ", Acceleration=" + Acceleration +
                    ", Year='" + Year + '\'' +
                    ", Origin='" + Origin + '\'' +
                    '}';
        }

        public Car(String Name, Double miles_per_Gallon, Long cylinders, Double displacement, Long horsepower, Long weight_in_lbs, Double acceleration, String year, String origin) {
            this.Name = Name;
            Miles_per_Gallon = miles_per_Gallon;
            Cylinders = cylinders;
            Displacement = displacement;
            Horsepower = horsepower;
            Weight_in_lbs = weight_in_lbs;
            Acceleration = acceleration;
            Year = year;
            Origin = origin;
        }

        public String getName() {
            return Name;
        }

        public void setName(String name) {
            this.Name = name;
        }

        public Double getMiles_per_Gallon() {
            return Miles_per_Gallon;
        }

        public void setMiles_per_Gallon(Double miles_per_Gallon) {
            Miles_per_Gallon = miles_per_Gallon;
        }

        public Long getCylinders() {
            return Cylinders;
        }

        public void setCylinders(Long cylinders) {
            Cylinders = cylinders;
        }

        public Double getDisplacement() {
            return Displacement;
        }

        public void setDisplacement(Double displacement) {
            Displacement = displacement;
        }

        public Long getHorsepower() {
            return Horsepower;
        }

        public void setHorsepower(Long horsepower) {
            Horsepower = horsepower;
        }

        public Long getWeight_in_lbs() {
            return Weight_in_lbs;
        }

        public void setWeight_in_lbs(Long weight_in_lbs) {
            Weight_in_lbs = weight_in_lbs;
        }

        public Double getAcceleration() {
            return Acceleration;
        }

        public void setAcceleration(Double acceleration) {
            Acceleration = acceleration;
        }

        public String getYear() {
            return Year;
        }

        public void setYear(String year) {
            Year = year;
        }

        public String getOrigin() {
            return Origin;
        }

        public void setOrigin(String origin) {
            Origin = origin;
        }
    }

}


