package com.actuallygr.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class UnionExample {

    public void performUnion() {

        SparkSession spark = SparkSession.builder()
                .appName("Combine 2 datasets")
                .master("local")
                .getOrCreate();

        Dataset<Row> durhamDf = buildDurhamParksDataSet(spark);
        Dataset<Row> philDf = buildPhilParksDataSet(spark);

        combineDataFrames(durhamDf, philDf);
    }

    public Dataset<Row> buildDurhamParksDataSet(SparkSession spark) {
        Dataset<Row> df = spark.read().format("json")
                .option("multiline", true)
                .load("src/main/resources/durham-parks.json");

        df = df.withColumn("park_id", concat(df.col("datasetid"), lit("_"), df.col("fields.objectId"), lit("_Durham")))
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("city", lit("Durham"))
                .withColumn("address", df.col("fields.address"))
                .withColumn("has_playground", df.col("fields.playground"))
                .withColumn("zipcode", df.col("fields.zip"))
                .withColumn("land_in_acres", df.col("fields.acres"))
                .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
                .withColumn("geoY", df.col("geometry.coordinates").getItem(1))
                .drop("fields").drop("geometry").drop("record_timestamp").drop("recordid").drop("datasetid");

        return df;
    }

    public Dataset<Row> buildPhilParksDataSet(SparkSession spark) {
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("multiline", true)
                .option("inferSchema", true)
                .load("src/main/resources/philadelphia_recreations.csv");

        df = df.filter(lower(df.col("USE_")).like("%park%"));
//        df = df.filter("lower(USE_) like '%park%'"); --> SQL way

        df = df.withColumn("park_id", concat(lit("PHIL_"), df.col("OBJECTID")))
                .withColumnRenamed("ASSET_NAME", "park_name")
                .withColumn("city", lit("Philadelphia"))
                .withColumnRenamed("ADDRESS","address")
                .withColumn("has_playground", lit("UNKNOWN"))
                .withColumnRenamed("ZIPCODE", "zipcode")
                .withColumnRenamed("ACREAGE", "land_in_acres")
                .withColumn("geoX", lit("UNKNOWN"))
                .withColumn("geoY", lit("UNKNOWN"))
                .drop("SITE_NAME").drop("OBJECTID").drop("CHILD_OF").drop("TYPE")
                .drop("USE_").drop("DESCRIPTION").drop("SQ_FEET").drop("DATE_EDITED")
                .drop("ALLIAS").drop("CHRONOLOGY").drop("NOTES").drop("OCCUPANT")
                .drop("TENANT").drop("LABEL").drop("EDITED_BY");



        return df;
    }

    public void combineDataFrames(Dataset<Row> df1, Dataset<Row> df2) {
        // match by column names using the unionBYName method
        // if only union() method then it matches based on order
        Dataset<Row> df = df1.unionByName(df2);
        df.show(10);
    }
}
