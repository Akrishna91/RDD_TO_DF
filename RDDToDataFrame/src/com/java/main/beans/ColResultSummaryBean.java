package com.java.main.beans;

import com.java.main.constants.RulesMatchingStatus;

public class ColResultSummaryBean {

	private String columnName;

	private RulesMatchingStatus uniquenessRuleResult;
	private RulesMatchingStatus possibleValueRuleResult;
	private RulesMatchingStatus dateTypeRuleResult;
	private RulesMatchingStatus summationRuleResult;
	private RulesMatchingStatus minimumRuleResult;
	private RulesMatchingStatus maximumRuleResult;
	private RulesMatchingStatus meanRuleResult;
	private RulesMatchingStatus modeRuleResult;
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public RulesMatchingStatus getUniquenessRuleResult() {
		return uniquenessRuleResult;
	}
	public void setUniquenessRuleResult(RulesMatchingStatus uniquenessRuleResult) {
		this.uniquenessRuleResult = uniquenessRuleResult;
	}
	public RulesMatchingStatus getPossibleValueRuleResult() {
		return possibleValueRuleResult;
	}
	public void setPossibleValueRuleResult(
			RulesMatchingStatus possibleValueRuleResult) {
		this.possibleValueRuleResult = possibleValueRuleResult;
	}
	public RulesMatchingStatus getDateTypeRuleResult() {
		return dateTypeRuleResult;
	}
	public void setDateTypeRuleResult(RulesMatchingStatus dateTypeRuleResult) {
		this.dateTypeRuleResult = dateTypeRuleResult;
	}
	public RulesMatchingStatus getSummationRuleResult() {
		return summationRuleResult;
	}
	public void setSummationRuleResult(RulesMatchingStatus summationRuleResult) {
		this.summationRuleResult = summationRuleResult;
	}
	public RulesMatchingStatus getMinimumRuleResult() {
		return minimumRuleResult;
	}
	public void setMinimumRuleResult(RulesMatchingStatus minimumRuleResult) {
		this.minimumRuleResult = minimumRuleResult;
	}
	public RulesMatchingStatus getMaximumRuleResult() {
		return maximumRuleResult;
	}
	public void setMaximumRuleResult(RulesMatchingStatus maximumRuleResult) {
		this.maximumRuleResult = maximumRuleResult;
	}
	public RulesMatchingStatus getMeanRuleResult() {
		return meanRuleResult;
	}
	public void setMeanRuleResult(RulesMatchingStatus meanRuleResult) {
		this.meanRuleResult = meanRuleResult;
	}
	public RulesMatchingStatus getModeRuleResult() {
		return modeRuleResult;
	}
	public void setModeRuleResult(RulesMatchingStatus modeRuleResult) {
		this.modeRuleResult = modeRuleResult;
	}



}
