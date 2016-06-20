package com.java.main.beans;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.spark.sql.Row;

public class FinalSummaryBean {
	
	
	
	private boolean finalStatus;
	private HashMap<String, ColResultSummaryBean> colResultSummaryBean;
	private HashMap<String, List<AggregationMismatches>> colNames_AggError;
	private List<Row> missingRows;
	private LinkedHashMap<String, RulesComparaorResult> distinctRuleResults;
	private LinkedHashMap<String, RulesComparaorResult> possibleValueRuleResults;
	
	public boolean isFinalStatus() {
		return finalStatus;
	}
	public void setFinalStatus(boolean finalStatus) {
		this.finalStatus = finalStatus;
	}
	public HashMap<String, ColResultSummaryBean> getColResultSummaryBean() {
		return colResultSummaryBean;
	}
	public void setColResultSummaryBean(HashMap<String, ColResultSummaryBean> colResultSummaryBean) {
		this.colResultSummaryBean = colResultSummaryBean;
	}
	public HashMap<String, List<AggregationMismatches>> getColNames_AggError() {
		return colNames_AggError;
	}
	public void setColNames_AggError(
			HashMap<String, List<AggregationMismatches>> colNames_AggError) {
		this.colNames_AggError = colNames_AggError;
	}
	public List<Row> getMissingRows() {
		return missingRows;
	}
	public void setMissingRows(List<Row> missingRows) {
		this.missingRows = missingRows;
	}
	public LinkedHashMap<String, RulesComparaorResult> getDistinctRuleResults() {
		return distinctRuleResults;
	}
	public void setDistinctRuleResults(
			LinkedHashMap<String, RulesComparaorResult> distinctRuleResults) {
		this.distinctRuleResults = distinctRuleResults;
	}
	public LinkedHashMap<String, RulesComparaorResult> getPossibleValueRuleResults() {
		return possibleValueRuleResults;
	}
	public void setPossibleValueRuleResults(
			LinkedHashMap<String, RulesComparaorResult> possibleValueRuleResults) {
		this.possibleValueRuleResults = possibleValueRuleResults;
	}

}
