package com.java.main.beans;

import com.java.main.constants.AggregationFuncNames;

public class AggregationMismatches {
	
	private AggregationFuncNames misMatchedFuncName;
	private String sourceValue;
	private String destValue;
	public AggregationFuncNames getMisMatchedFuncName() {
		return misMatchedFuncName;
	}
	public void setMisMatchedFuncName(AggregationFuncNames misMatchedFuncName) {
		this.misMatchedFuncName = misMatchedFuncName;
	}
	public String getSourceValue() {
		return sourceValue;
	}
	public void setSourceValue(String sourceValue) {
		this.sourceValue = sourceValue;
	}
	public String getDestValue() {
		return destValue;
	}
	public void setDestValue(String destValue) {
		this.destValue = destValue;
	}

}
