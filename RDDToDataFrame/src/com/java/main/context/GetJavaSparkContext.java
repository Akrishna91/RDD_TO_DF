package com.java.main.context;

import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Class which helps in creating Spark and Java Spark Contexts
 * 
 * @author Karthik
 *
 */

public class GetJavaSparkContext implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3390423101644562774L;

	private static JavaSparkContext sc = null;

	public static JavaSparkContext getSc() {
		return sc;
	}

	public static void setSc(JavaSparkContext sc) {
		GetJavaSparkContext.sc = sc;
	}

	public static JavaSparkContext getJavaSparkContex() {
		if (sc == null) {
			// Create a java spark context
			SparkConf conf = new SparkConf().setMaster("local").setAppName(
					"DQIntegrator");
			Logger log = Logger.getLogger("org");
			Logger log1 = Logger.getLogger("akka");
			log.setLevel(Level.WARN);
			log1.setLevel(Level.WARN);
			conf.set("spark.driver.allowMultipleContexts", "true");
			conf.set("spark.logConf", "true");
			sc = new JavaSparkContext(conf);
			sc.hadoopConfiguration().setInt("parquet.metadata.read.parallelism", 10);
		}
		return sc;
	}

	public static SparkContext getSparkContex() {
		// Create a Spark context
		SparkConf conf = new SparkConf().setMaster("local").setAppName(
				"DQIntegrator");
		Logger log = Logger.getLogger("org");
		Logger log1 = Logger.getLogger("akka");
		log.setLevel(Level.WARN);
		log1.setLevel(Level.WARN);
		conf.set("spark.driver.allowMultipleContexts", "true");
		conf.set("spark.logConf", "true");
		SparkContext sc = new SparkContext(conf);
		return sc;
	}
}
