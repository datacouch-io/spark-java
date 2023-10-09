package com.sparkTutorial.part3foundations;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.upper;

public class ColumnPruning {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("Different Spark APIs")
                .master("local[1]").getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


        Dataset<Row> guitarsDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/guitars/guitars.json");

        Dataset<Row> guitarPlayersDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/guitarPlayers/guitarPlayers.json");

        Dataset<Row>  bandsDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/bands/bands.json");

        Column joinCondition = guitarPlayersDF.col("band").equalTo(bandsDF.col("id"));
        Dataset<Row> guitaristsBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "inner");
        guitaristsBandsDF.explain();

        Dataset<Row> guitaristsWithoutBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "left_anti");
        guitaristsWithoutBandsDF.explain();

        /*
    == Physical Plan ==
    *(2) BroadcastHashJoin [band#22L], [id#38L], LeftAnti, BuildRight
    :- *(2) Project [band#22L, guitars#23, id#24L, name#25] <- UNNECESSARY
    :  +- BatchScan[band#22L, guitars#23, id#24L, name#25] JsonScan Location: InMemoryFileIndex[file:/Users/daniel/dev/rockthejvm/courses/spark-optimization/src/main/resources..., ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
    +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#66]
       +- *(1) Project [id#38L] <- COLUMN PRUNING
          +- *(1) Filter isnotnull(id#38L)
             +- BatchScan[id#38L] JsonScan Location: InMemoryFileIndex[file:/Users/daniel/dev/rockthejvm/courses/spark-optimization/src/main/resources..., ReadSchema: struct<id:bigint>

    Column pruning = cut off columns that are not relevant
    = shrinks DF
    * useful for joins and groups
   */

    // project and filter pushdown
    Dataset<Row> namesDF = guitaristsBandsDF.select(guitarPlayersDF.col("name"), bandsDF.col("name"));
    namesDF.explain()  ;

    /*
    Spark tends to drop columns as early as possible.
  Should be YOUR goal as well.
     */

    Dataset<Row> rockDF = guitarPlayersDF
            .join(bandsDF, joinCondition)
            .join(guitarsDF, array_contains(guitarPlayersDF.col("guitars"), guitarsDF.col("id")));


        Dataset<Row> essentialsDF = rockDF.select(guitarPlayersDF.col("name"), bandsDF.col("name"), upper(guitarsDF.col("make")));
        essentialsDF.explain();

        /**
         * LESSON: if you anticipate that the joined table is much larger than the table on whose column you are applying the
         * map-side operation, e.g. " * 5", or "upper", do this operation on the small table FIRST.
         *
         * Particularly useful for outer joins.
         */

    }
    }
