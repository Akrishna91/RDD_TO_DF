package com.java.main.businessrules.cardinality;

import com.java.main.context.GetJavaSparkContext;

public class Driver {
	
	
	public static void main(String[] args){
		String ipPath = "/home/cloudera/Desktop/Greenplum5Million.dat";
		Cardinality cardinality = new Cardinality(GetJavaSparkContext.getJavaSparkContex(), ipPath);
		cardinality.getCardinality();
	}

}
