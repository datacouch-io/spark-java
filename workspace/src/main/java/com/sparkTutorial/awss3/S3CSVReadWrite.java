package com.sparkTutorial.awss3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;

public class S3CSVReadWrite {

    public static void main(String[] args) {
        // Initialize Spark
        SparkConf conf = new SparkConf()
                .setAppName("S3CSVReadWrite")
                .setMaster("local[*]"); // You can change this to a cluster URL if needed


        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder()
                .appName("S3CSVReadWrite")
                .getOrCreate();

        // S3 Configuration
        String awsAccessKeyId = "awsAccessKeyId";
        String awsSecretAccessKey = "awsSecretAccessKey";
        String s3Bucket = "demo0123";
        String s3Key = "costs.csv";

        // Set AWS credentials
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", awsAccessKeyId);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", awsSecretAccessKey);
        spark.sparkContext().hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"); // Add this line
        spark.sparkContext().hadoopConfiguration().set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com");
        // Read CSV from S3 into DataFrame
        String s3Path = "s3a://" + s3Bucket + "/" + s3Key;
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(s3Path);

        // Show DataFrame contents
        df.show();

        // Perform operations on the DataFrame here...

        // Write DataFrame back to S3 as CSV
        String outputS3Key = "file.csv";
        df.write()
                .mode(SaveMode.Overwrite) // Change this mode as needed
                .option("header", "true")
                .csv("s3a://" + s3Bucket + "/" + outputS3Key);

        // Stop SparkContext
        spark.stop();
    }
}
