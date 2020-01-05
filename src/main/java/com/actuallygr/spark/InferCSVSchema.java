package com.actuallygr.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class InferCSVSchema {

    public void printSchema() {
        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        // get data
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("inferSchema", true)
                .load("src/main/resources/2015-summary.csv");

//		df.show(3);

//		df = df.withColumn("full_name",
//				concat(df.col("last_name"), lit(" , "), df.col("first_name")))
//				.filter(df.col("comment").rlike("\\d+"))
//				.orderBy(df.col("last_name").desc());

        df.printSchema();
        df.show();

        String dbConnectionUrl = "jdbc:postgresql://localhost:5432/postgres";
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "passlugogod3825");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "project1", prop);

    }
}
