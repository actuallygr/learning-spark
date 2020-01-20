package com.actuallygr.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class CapstoneProject {

    public void start() {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();

        Dataset<Row> emailDf = spark.read().format("csv")
                .option("multiline", true)
                .option("inferSchema", true)
                .option("header", true)
                .load("src/main/resources/CampaignData_full-2.csv");

        emailDf.printSchema();

        Dataset<Row> language = emailDf.select("LANGUAGE");

        System.out.println(language.where("LANGUAGE == '20' OR LANGUAGE == '17' " ).count()); // problem 1

        Dataset<Row> problem2 = emailDf.select("COUNTRY_OF_ORIGIN","NEW_CAR_MODEL").where("COUNTRY_OF_ORIGIN == 02 AND NEW_CAR_MODEL == 1"); //problem 2

        System.out.println(problem2.count());

        Dataset<Row> openEmailDF = emailDf.select("OPEN_FLG", "Mailed_Date").where("OPEN_FLG == 'Y' ");

        System.out.println(openEmailDF.where("Mailed_Date LIKE '%Mon%' OR Mailed_Date LIKE '%Fri%' ").count()); // problem 3

        Dataset<Row> educationDf = emailDf.select("INDIV_EDUCATION").groupBy("INDIV_EDUCATION").count(); // problem 4

        educationDf.coalesce(1).write().mode(SaveMode.Overwrite).json("J:/IntellijProjects/learning-spark/src/main/resources/education.json");

        Dataset<Row> ruralDf = emailDf.select("DWELLING_TYPE", "OPEN_FLG", "CLICK_FLG").where("DWELLING_TYPE == 'R'");

        Column condOpen = when(col("OPEN_FLG").equalTo("Y"), 1)
                .otherwise(0);

        Column condClick = when(col("CLICK_FLG").equalTo("Y"), 1)
                .otherwise(0);

        ruralDf = ruralDf.withColumn("open", condOpen).drop("OPEN_FLG");
        ruralDf = ruralDf.withColumn("click", condClick).drop("CLICK_FLG");

        ruralDf.agg(sum("click").divide(sum("open")).multiply(100).as("Rural CTOR")).show();

        ruralDf.show(); // problem 5

        Dataset<Row> mfRatioDf = emailDf.select("INDIV_MARITAL_STATUS", "I1_GNDR_CODE").where("INDIV_MARITAL_STATUS == 'M' OR INDIV_MARITAL_STATUS == 'S'");

        mfRatioDf = mfRatioDf.where("I1_GNDR_CODE == 'F' OR I1_GNDR_CODE == 'M'");

        Column condMarriedStatus = when(col("INDIV_MARITAL_STATUS").equalTo("M"), 1).otherwise(0);

        Column condSingleStatus = when(col("INDIV_MARITAL_STATUS").equalTo("S"), 1).otherwise(0);

        mfRatioDf = mfRatioDf.withColumn("Married_status", condMarriedStatus).withColumn("Single_status", condSingleStatus).drop("INDIV_MARITAL_STATUS");

        mfRatioDf.groupBy("I1_GNDR_CODE").agg(sum("Married_status").divide(sum("Single_status")).as("Ration Married to Single")).show(); //problem 6

        Dataset<Row> mostActive = emailDf.select("OPEN_FLG", "Mailed_Date").where("OPEN_FLG == 'Y' ");

        Column condDay = when(col("Mailed_Date").like("%Mon%"), "MON").when(col("Mailed_Date").like("%Tue%"), "TUE").when(col("Mailed_Date").like("%Wed%"), "WED")
                .when(col("Mailed_Date").like("%Thu%"), "THU").when(col("Mailed_Date").like("%Fri%"), "FRI").when(col("Mailed_Date").like("%Sat%"), "SAT").otherwise("SUN");

        mostActive = mostActive.withColumn("day", condDay).drop("Mailed_Date");

        mostActive = mostActive.groupBy("day").count().as("Day_count");

        mostActive.show();

        mostActive.coalesce(1).write().mode(SaveMode.Overwrite).json("J:/IntellijProjects/learning-spark/src/main/resources/mostactive.json");


        Dataset<Row> topIncomeDf = emailDf.select("EXPERIAN_INCOME_CD", "OPEN_FLG", "CLICK_FLG").where("EXPERIAN_INCOME_CD == 'H' OR EXPERIAN_INCOME_CD == 'I' OR EXPERIAN_INCOME_CD == 'J' OR EXPERIAN_INCOME_CD == 'K' OR EXPERIAN_INCOME_CD == 'L' ");

        topIncomeDf = topIncomeDf.withColumn("open", condOpen).drop("OPEN_FLG");
        topIncomeDf = topIncomeDf.withColumn("click", condClick).drop("CLICK_FLG");

        topIncomeDf.groupBy("EXPERIAN_INCOME_CD").agg(sum("click").divide(sum("open")).multiply(100).as("CTOR")).show();
    }
}
