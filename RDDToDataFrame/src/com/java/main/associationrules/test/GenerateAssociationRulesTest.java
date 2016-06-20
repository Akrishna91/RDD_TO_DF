package com.java.main.associationrules.test;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;

import com.java.main.associationrules.src.GenerateAssociationRules;
import com.java.main.context.GetJavaSparkContext;

public class GenerateAssociationRulesTest implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1165577245675298549L;

	public static void main(String[] args){
		GenerateAssociationRulesTest test = new GenerateAssociationRulesTest();
		String ipPath = args[0];
		double minSupport = 0.2;
		double minConfidence = 0.8;
		GenerateAssociationRules generateAssociationRules = new GenerateAssociationRules(ipPath, minSupport, minConfidence, test.getContext());
		generateAssociationRules.generateRules();
	}

	private JavaSparkContext getContext() {
		JavaSparkContext jsc = GetJavaSparkContext.getJavaSparkContex();
		return jsc;
	}

}
