package com.actuallygr.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class JoiningDataFrames {

    public void start() {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();

        Dataset<Row> studentDf = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load("src/main/resources/students.csv");

        Dataset<Row> gradesDf = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load("src/main/resources/grade_chart.csv");

        Dataset<Row> joinedDf = studentDf.join(gradesDf,studentDf.col("GPA").equalTo(gradesDf.col("gpa")))
                .filter(gradesDf.col("gpa").between(2,3.5))
                .select(col("student_name"), col("favorite_book_title"), col("letter_grade"));
        joinedDf.show();

    }
}
