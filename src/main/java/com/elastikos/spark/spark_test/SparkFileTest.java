package com.elastikos.spark.spark_test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkFileTest {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("SparkExample").setMaster("local[*]");

		JavaSparkContext context = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = context.textFile("/Users/Venkata/Documents/test/test.txt");

		JavaRDD<String> linesWithSpark = lines.filter(new Function<String, Boolean>() {

			public Boolean call(String line) {
				return line.contains("spark");
			}

		});
		
		System.out.println(" Count is : " + linesWithSpark.count());
		linesWithSpark.foreach(f -> System.out.println("Prininting line containing spark: " + f)); 

		context.close();

	}

}
