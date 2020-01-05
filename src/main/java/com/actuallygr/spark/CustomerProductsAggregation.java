package com.actuallygr.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CustomerProductsAggregation {

    public void start() {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();

        Dataset<Row> customersDf = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load("src/main/resources/customers.csv");

        Dataset<Row> productsDf = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load("src/main/resources/products.csv");

        Dataset<Row> purchasesDf = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load("src/main/resources/purchases.csv");

        Dataset<Row> joinedDf = customersDf.join(purchasesDf, customersDf.col("customer_id").equalTo(purchasesDf.col("customer_id")))
                .join(productsDf, purchasesDf.col("product_id").equalTo(productsDf.col("product_id")))
                .drop("favorite_website").drop(purchasesDf.col("customer_id")).drop("product_id");

        joinedDf.groupBy("first_name").agg(count("product_name").as("number_of_purchases"), max("product_price").as("most_exp_purchase"),
                sum("product_price").as("total_spent")).show();

        joinedDf.show();
    }
}
