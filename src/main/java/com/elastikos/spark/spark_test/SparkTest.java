package com.elastikos.spark.spark_test;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkTest {

	public static void main(String[] args) {

		List<Person> testList = Arrays.asList(new Person( "John", "Walter"), new Person("Jane", "Doe"), new Person("Ram", "Doe"));

		SparkConf sparkConf = new SparkConf().setAppName("SparkExample").setMaster("local[*]");

		JavaSparkContext context = new JavaSparkContext(sparkConf);

		JavaRDD<Person> testRDD = context.parallelize(testList);
		JavaRDD<Person> testRDD1 = testRDD.filter(new Function<Person, Boolean>(){
			public Boolean call(Person person) {
				return person.getLastName().equalsIgnoreCase("Doe");
			}
		});
		
		System.out.println("------- priniting RDD ------");
		testRDD1.foreach((f) -> System.out.println(f));
		
		context.close();

	}

}
