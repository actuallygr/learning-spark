package com.actuallygr.spark;

import com.actuallygr.mapper.HouseMapper;
import com.actuallygr.pojos.House;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;

public class CsvToDatasetHouseToDataframe {

    public void start() {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", "true")
                .option("sep", ";")
                .load("src/main/resources/houses.csv");

        df.show(5);
        df.printSchema();

        Dataset<House> houseDs = df.map(new HouseMapper(), Encoders.bean(House.class));

        houseDs.printSchema();
        houseDs.show();

        Dataset<Row> houseDf = houseDs.toDF();
//        houseDf.withColumn("formatedDate", concat( ))
        houseDf.printSchema();
    }

}
