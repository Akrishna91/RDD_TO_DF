package com.java.main.processor;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.java.main.beans.SampleSchema;
public class StaticSchema {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("sample")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// sc is an existing JavaSparkContext.
		org.apache.spark.sql.SQLContext sqlContext = new org.apache.spark.sql.SQLContext(
				sc);

		// Load a text file and convert each line to a JavaBean.
		JavaRDD<SampleSchema> sample = sc.textFile(
				"/home/cloudera/Desktop/sampleCSV").map(
				new Function<String, SampleSchema>() {
					/**
		 * 
		 */
					private static final long serialVersionUID = 1L;

					public SampleSchema call(String line) throws Exception {
						String[] parts = line.split(",");

						SampleSchema sample = new SampleSchema();
						sample.set_id(Integer.parseInt(parts[0].trim()));
						sample.setName(parts[1].trim());

						return sample;
					}
				});

		// Apply a schema to an RDD of JavaBeans and register it as a table.
		DataFrame sampleSchema = sqlContext.createDataFrame(sample,
				SampleSchema.class);
		sampleSchema.registerTempTable("sample");

		// SQL can be run over RDDs that have been registered as tables.
		DataFrame names = sqlContext.sql("SELECT _id FROM sample");
		for(String col: sampleSchema.columns()){
			System.out.println(col);
		}
		// The results of SQL queries are DataFrames and support all the normal
		// RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		List<String> namesList = names.javaRDD()
				.map(new Function<Row, String>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = -5552987624028457332L;

					public String call(Row row) {
						return "Name: " + row.getInt(0);
					}
				}).collect();
		System.out.println(namesList);
		names.show();
	}

}
