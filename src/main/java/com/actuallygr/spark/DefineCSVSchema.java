package com.actuallygr.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DefineCSVSchema {

    public void printDefinedSchema() {
        SparkSession spark = SparkSession.builder()
                .appName("Complext CSV to DataFrame")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("DEST_COUNTRY_NAME", DataTypes.StringType, true),
                DataTypes.createStructField("ORIGIN_COUNTRY_NAME", DataTypes.StringType, true),
                DataTypes.createStructField("count", DataTypes.IntegerType, true)
        });

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .schema(schema)
                .load("src/main/resources/2015-summary.csv");

        df.show(5, 15);
        df.printSchema();
    }
}
