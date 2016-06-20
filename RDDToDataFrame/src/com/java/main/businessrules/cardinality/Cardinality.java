package com.java.main.businessrules.cardinality;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.java.main.constants.ValueSeparater;
import com.java.main.utils.RDDUtils;

import scala.Tuple2;

public class Cardinality implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7247122890354959436L;
	transient private JavaSparkContext jsc;
	private String ipPath;

	public Cardinality(JavaSparkContext jsc, String ipPath) {
		this.jsc = jsc;
		this.ipPath = ipPath;
	}

	public void getCardinality() {
		JavaRDD<String> data = jsc.textFile(ipPath);
		final String[] headers = data.first().split(ValueSeparater.getRegex());
		JavaRDD<String> valuesWihtoutHeaders = RDDUtils.removeHeaders(data);
		JavaRDD<String> aftGrouping = valuesWihtoutHeaders
				.map(new Function<String, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public String call(String arg) throws Exception {
						String rowValue = "";
						String[] row = arg.split(ValueSeparater.getRegex());
						for (int i = 0; i < row.length; i++) {
							row[i] = headers[i]+ValueSeparater.getRegex()+row[i] ;
							rowValue += row[i] + ValueSeparater.getRegexForCardinality();
						}
						return rowValue;
					}
				});
		JavaRDD<String> words = aftGrouping
				.flatMap(new FlatMapFunction<String, String>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Iterable<String> call(String x) {
						return Arrays.asList(x.split(ValueSeparater.getRegexForCardinality()));
					}
				});
		JavaPairRDD<String, String>  pairs = words.mapToPair(new PairFunction<String, String, String>() {
		
			/**
			 * 
			 */
			private static final long serialVersionUID = 3860669960653090605L;

			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				if(arg0.trim().length() > 0){
					String[] values = arg0.split(ValueSeparater.getRegex(), -1);
					try{
						return new Tuple2<String, String>(values[0],values[1]);
					}catch (Exception e){
						System.out.println("values0 " + values[0]);
						e.printStackTrace();
					}
					
				}
				return new Tuple2<String, String>(ValueSeparater.getRegex(), ValueSeparater.getRegex());
			}
		});
		//System.out.println("pairs count-->"+pairs.count());
		JavaPairRDD<String, Object> counts = pairs.countApproxDistinctByKey(0.01);
		for(Tuple2<String, Object> count : counts.collect()){
			System.out.println("Column Name-->"+count._1+" ==> Cardinality-->"+count._2);
		}
	}

	
	public JavaSparkContext getJsc() {
		return jsc;
	}

	public void setJsc(JavaSparkContext jsc) {
		this.jsc = jsc;
	}

	public String getIpPath() {
		return ipPath;
	}

	public void setIpPath(String ipPath) {
		this.ipPath = ipPath;
	}

}
