package com.actuallygr.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class JSONLinesParser {

    public void parseJsonLines() {

        SparkSession spark = SparkSession.builder()
                .appName("json parser")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("json")
                .option("headers", true)
                .load("src/main/resources/2015-summary.json");

        df.show();
        df.printSchema();
    }
}
