package com.actuallygr.spark;

import com.actuallygr.utils.WordsUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class RedditAnalyzer {

    public void start() {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();

        Dataset<Row> redditDf = spark.read().format("json")
                .option("inferSchema" , true)
                .option("header", true)
                .load("src/main/resources/1.2 Reddit_2007-small.json.json");

        redditDf = redditDf.select("body");

        Dataset<String> wordsDs = redditDf.flatMap((FlatMapFunction<Row, String>)
                r -> Arrays.asList(r.toString().toLowerCase().replace("\n", "").replace("\r","").replace("[","")
                        .replace(".","").replace("]", "").replace("-","")
                        .trim().split(" ")).iterator()
                ,Encoders.STRING());

        Dataset<Row> wordsDf = wordsDs.toDF();

        Dataset<Row> boringWordsDf = spark.createDataset(Arrays.asList(WordsUtils.stopWords), Encoders.STRING()).toDF();

//        wordsDf = wordsDf.except(boringWordsDf); // removes duplicates from the end dataframe

        wordsDf = wordsDf.join(boringWordsDf, wordsDf.col("value").equalTo(boringWordsDf.col("value")), "left_anti");

        wordsDf = wordsDf.groupBy("value").count();

        wordsDf = wordsDf.orderBy(wordsDf.col("count").desc());

        wordsDf.show();

    }
}
