package com.actuallygr.spark;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import scala.reflect.internal.Trees;

public class Application {
	
	public static void main(String args[]) throws InterruptedException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

//		InferCSVSchema schema = new InferCSVSchema();
//		schema.printSchema();

//		DefineCSVSchema defineCSVSchema = new DefineCSVSchema();
//		defineCSVSchema.printDefinedSchema();

//		JSONLinesParser jsonLinesParser = new JSONLinesParser();
//		jsonLinesParser.parseJsonLines();

//		UnionExample unionExample = new UnionExample();
//		unionExample.performUnion();

//		ArrayToDataset arrayToDataset = new ArrayToDataset();
//		arrayToDataset.start();

//		CsvToDatasetHouseToDataframe csvToDatasetHouseToDataframe = new CsvToDatasetHouseToDataframe();
//		csvToDatasetHouseToDataframe.start();

//		WordCount wordCount = new WordCount();
//		wordCount.start();

//		JoiningDataFrames joiningDataFrames = new JoiningDataFrames();
//		joiningDataFrames.start();

//		CustomerProductsAggregation customerProductsAggregation = new CustomerProductsAggregation();
//		customerProductsAggregation.start();

//		RedditAnalyzer redditAnalyzer = new RedditAnalyzer();
//		redditAnalyzer.start();

//		StreamingSocketApp streamingSocketApp = new StreamingSocketApp();
//		streamingSocketApp.start();

//		JigsawEmailNG jigsawEmailNG = new JigsawEmailNG();
//		jigsawEmailNG.start();

		AssignmentRdd assignmentRdd = new AssignmentRdd();
		assignmentRdd.start();
	}
}
