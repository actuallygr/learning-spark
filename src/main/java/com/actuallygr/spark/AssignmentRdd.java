package com.actuallygr.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class AssignmentRdd {

    public void start() {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("year", DataTypes.IntegerType, true),
                DataTypes.createStructField("length", DataTypes.IntegerType, true),
                DataTypes.createStructField("title", DataTypes.StringType, true),
                DataTypes.createStructField("subject", DataTypes.StringType, true),
                DataTypes.createStructField("actor", DataTypes.StringType, true),
                DataTypes.createStructField("actores", DataTypes.StringType, true),
                DataTypes.createStructField("director", DataTypes.StringType, true),
                DataTypes.createStructField("popularity", DataTypes.IntegerType, true),
                DataTypes.createStructField("award", DataTypes.StringType, true),
                DataTypes.createStructField("file_name", DataTypes.StringType, true)
        });

        Dataset<Row> movieDf = spark.read().format("csv")
                .schema(schema)
                .option("sep",";")
                .option("multiline", true)
                .load("src/main/resources/movie_datset.txt");

        movieDf = movieDf.withColumn("decade", col("year").minus(1900).divide(10).cast(DataTypes.IntegerType));
        movieDf = movieDf.drop("subject").drop("actor").drop("actores").drop("director").drop("award").drop("file_name").drop("length");

        Dataset<Row> maxPopularity = movieDf.groupBy(col("decade")).agg(max("popularity").as("popularity"));

        maxPopularity.show();

        Dataset<Row> joinedDf = movieDf.join(maxPopularity, movieDf.col("popularity").equalTo(maxPopularity.col("popularity")).and(movieDf.col("decade").equalTo(maxPopularity.col("decade"))))
                .orderBy(movieDf.col("decade").desc())
                .drop(movieDf.col("decade"))
                .drop(movieDf.col("popularity"))
                .drop(movieDf.col("year"));

        joinedDf.show();
    }
}
