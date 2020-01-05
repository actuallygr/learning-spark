package com.actuallygr.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ArrayToDataset {

    public void start() {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();

        String[] stringList = new String[] {"Banana", "Car", "Glass", "Banana", "Computer", "Car"};

        List<String> data = Arrays.asList(stringList);

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        Dataset<Row> df = ds.toDF();

        df.as(Encoders.STRING());

        ds.printSchema();
        ds.show();

        Dataset<Row> df2 = ds.groupBy("value").count();

        df.printSchema();
        df.show();

        //mapping and reducing

//        ds = ds.map(new StringMapper(), Encoders.STRING());

        ds = ds.map((MapFunction<String, String>) row -> "word: "+ row, Encoders.STRING());
        ds.show();

        String string = ds.reduce(new StringReducer());
        System.out.println(string);

    }

    public static class StringMapper implements MapFunction<String ,String>, Serializable {

        @Override
        public String call(String s) throws Exception {
            return "word: " + s;
        }
    }

    public static class StringReducer implements ReduceFunction<String>, Serializable {

        @Override
        public String call(String s, String t1) throws Exception {
            return s + t1;
        }
    }
}
