package com.sparkTutorial.part3foundations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadingQueryPlans {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("Reading query plans")
                .master("local[1]").getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(session.sparkContext());

        // plan 1 - a simple transformation
        Dataset<Long> simpleNumbers = session.range(1, 1000000);
        Dataset<Row> times5 = simpleNumbers.selectExpr("id * 5 as id");
        times5.explain(true); // this is how you show a query plan
        times5.show();

        Thread.sleep(1000000);

/*
        // plan 2 - a shuffle
        Dataset<Long> moreNumbers = session.range(1, 1000000, 2);
        Dataset<Long> split7 = moreNumbers.repartition(7);
        *//*
        == Physical Plan ==
        Exchange RoundRobinPartitioning(7), false, [id=#16]
        +- *(1) Range (1, 1000000, step=2, splits=6)
       *//*

        // plan 3 - shuffle + transformation
        split7.selectExpr("id * 5 as id").explain();
      *//*
        == Physical Plan ==
        *(2) Project [(id#4L * 5) AS id#8L]
        +- Exchange RoundRobinPartitioning(7), false, [id=#29]
          +- *(1) Range (1, 1000000, step=2, splits=6)
       *//*

        // plan 4 - a more complex job with a join
        Dataset<Long> ds1 = session.range(1, 10000000);
        Dataset<Long> ds2 = session.range(1, 20000000, 2);
        Dataset<Long> ds3 = ds1.repartition(7);
        Dataset<Long> ds4 = ds2.repartition(9);
        Dataset<Row> ds5 = ds3.selectExpr("id * 3 as id");
        Dataset<Row> joined = ds5.join(ds4, "id");
        Dataset<Row> sum = joined.selectExpr("sum(id)");
        sum.explain();

  *//*== Physical Plan ==
  (7) HashAggregate(keys=[], functions=[sum(id#18L)])
        +- Exchange SinglePartition, true, [id=#99]
        +- *(6) HashAggregate(keys=[], functions=[partial_sum(id#18L)])
        +- *(6) Project [id#18L]
        +- *(6) SortMergeJoin [id#18L], [id#12L], Inner
        :- *(3) Sort [id#18L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#18L, 200), true, [id=#83]
          :     +- *(2) Project [(id#10L * 3) AS id#18L]
          :        +- Exchange RoundRobinPartitioning(7), false, [id=#79]
          :           +- *(1) Range (1, 10000000, step=1, splits=6)
                +- *(5) Sort [id#12L ASC NULLS FIRST], false, 0
                +- Exchange hashpartitioning(id#12L, 200), true, [id=#90]
        +- Exchange RoundRobinPartitioning(9), false, [id=#89]
        +- *(4) Range (1, 20000000, step=2, splits=6)
                *//*

        *//**
         * Exercises - read the Query Plans and try to understand the code that generated them.
         *//*

        // exercise 1
          *//*
            == Physical Plan ==
            *(1) Project [firstName#153, lastName#155, (cast(salary#159 as double) / 1.1) AS salary_EUR#168]
            +- *(1) FileScan csv [firstName#153,lastName#155,salary#159] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/tmp/employees_headers.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<firstName:string,lastName:string,salary:string>
           *//*

        Dataset<Row> employeesDF = session.read().option("header", true).csv("src/main/resources/data/employees_headers/employees_headers.csv");
        Dataset<Row> empEur = employeesDF.selectExpr("firstName", "lastName", "salary / 1.1 as salary_EUR") ;

        // exercise 2
          *//*
          == Physical Plan ==
          *(2) HashAggregate(keys=[dept#156], functions=[avg(cast(salary#181 as bigint))])
            +- Exchange hashpartitioning(dept#156, 200)
              +- *(1) HashAggregate(keys=[dept#156], functions=[partial_avg(cast(salary#181 as bigint))])
                +- *(1) Project [dept#156, cast(salary#159 as int) AS salary#181]
                  +- *(1) FileScan csv [dept#156,salary#159] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/tmp/employees_headers.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<dept:string,salary:string>
           *//*
        Dataset<Row> avgSals = employeesDF
                .selectExpr("dept", "cast(salary as int) as salary")
                .groupBy("dept")
                .avg("salary");


        // exercise 3
      *//*
      == Physical Plan ==
      *(5) Project [id#195L]
        +- *(5) SortMergeJoin [id#195L], [id#197L], Inner
          :- *(2) Sort [id#195L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#195L, 200)
          :     +- *(1) Range (1, 10000000, step=3, splits=6)
          +- *(4) Sort [id#197L ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(id#197L, 200)
              +- *(3) Range (1, 10000000, step=5, splits=6)
       *//*
        Dataset<Long> d1 = session.range(1, 10000000, 3);
        Dataset<Long> d2 = session.range(1, 10000000, 5);
        Dataset<Row> j1 = d1.join(d2, "id");*/
    }
    }
