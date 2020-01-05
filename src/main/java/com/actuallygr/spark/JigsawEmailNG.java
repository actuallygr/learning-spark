package com.actuallygr.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class JigsawEmailNG {

    public void start() {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();

        Dataset<Row> custDf = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .option("sep",",")
                .option("multiline", true)
                .load("src/main/resources/customer_acd.csv");

        Column condClick = when(col("click").equalTo("Y"), 1)
                .otherwise(0);

        Column condOpen = when(col("open").equalTo("Y"), 1)
                .otherwise(0);

        custDf = custDf.withColumn("click_num", condClick)
                .withColumn("open_num", condOpen)
                .drop("click").drop("open");

        custDf.agg(sum("click_num").divide(sum("open_num")).multiply(100).as("Overall CTOR")).show(); // Overall CTOR

        custDf.groupBy("Gender").agg(sum("click_num").divide(sum("open_num")).multiply(100).as("CTOR by Gender")).show(); // CTOR by Gender

        custDf.show();
    }
}
