package com.java.main.associationrules.src;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

public class GenerateAssociationRules implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7743692150615859991L;
	
	private String ipPath;
	private double minSupport;
	private double minConfidence;
	transient private JavaSparkContext jsc;

	public GenerateAssociationRules(String ipPath, double minSupport,
			double minConfidence, JavaSparkContext jsc) {
		this.ipPath = ipPath;
		this.minSupport = minSupport;
		this.minConfidence = minConfidence;
		this.jsc = jsc;
	}

	/**
	 * @return
	 */
	public AssociationRule generateRules() {
		JavaRDD<String> data = jsc.textFile(ipPath);
		JavaRDD<HashSet<String>> transactions = data
				.map(new Function<String, HashSet<String>>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 1L;

					public HashSet<String> call(String line) {
						String[] parts = line.split(",");
						
						return new HashSet<String>(Arrays.asList(parts));
					}
				});

		FPGrowth fpg = new FPGrowth().setMinSupport(minSupport)
				.setNumPartitions(10);
		FPGrowthModel<String> model = fpg.run(transactions);

		for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
			System.out.println("[" + itemset.javaItems() + "], "+ itemset.freq());
		}

		for (AssociationRules.Rule<String> rule : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
			System.out.println(rule.javaAntecedent() + " => "+ rule.javaConsequent() + ", " + rule.confidence());
		}
		
		return new AssociationRule(model.freqItemsets().toJavaRDD().collect(), model.generateAssociationRules(minConfidence).toJavaRDD().collect());
	}

	public String getIpPath() {
		return ipPath;
	}

	public void setIpPath(String ipPath) {
		this.ipPath = ipPath;
	}

	public double getMinSupport() {
		return minSupport;
	}

	public void setMinSupport(double minSupport) {
		this.minSupport = minSupport;
	}

	public double getMinConfidence() {
		return minConfidence;
	}

	public void setMinConfidence(double minConfidence) {
		this.minConfidence = minConfidence;
	}

	public JavaSparkContext getJavaSparkContext() {
		return jsc;
	}

	public void setJavaSparkContext(JavaSparkContext jsc) {
		this.jsc = jsc;
	}

}
