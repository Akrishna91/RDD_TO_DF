package com.java.main.ui;

import java.util.ArrayList;

public class ConfigurationDetailsBean {
	
	private Integer randomizationPrecentage;
	
	private ArrayList<String> columnNames;
	
	private String keyType;
	private ArrayList<String> keyColumnNames;
	
	private boolean uniquenessRule;
	private boolean possibleValueRule;
	private boolean dateTypeRule;
	private boolean summationRule;
	private boolean minimumRule;
	private boolean maximumRule;
	private boolean meanRule;
	private boolean modeRule;
	
	private ConnectionDetailsBean connectionDetailsBean;
	
	public Integer getRandomizationPrecentage() {
		return randomizationPrecentage;
	}
	public void setRandomizationPrecentage(Integer randomizationPrecentage) {
		this.randomizationPrecentage = randomizationPrecentage;
	}
	public ArrayList<String> getColumnNames() {
		return columnNames;
	}
	public void setColumnNames(ArrayList<String> columnNames) {
		this.columnNames = columnNames;
	}
	public boolean isUniquenessRule() {
		return uniquenessRule;
	}
	public void setUniquenessRule(boolean uniquenessRule) {
		this.uniquenessRule = uniquenessRule;
	}
	public boolean isPossibleValueRule() {
		return possibleValueRule;
	}
	public void setPossibleValueRule(boolean possibleValueRule) {
		this.possibleValueRule = possibleValueRule;
	}
	public boolean isDateTypeRule() {
		return dateTypeRule;
	}
	public void setDateTypeRule(boolean dateTypeRule) {
		this.dateTypeRule = dateTypeRule;
	}
	public boolean isSummationRule() {
		return summationRule;
	}
	public void setSummationRule(boolean summationRule) {
		this.summationRule = summationRule;
	}
	public boolean isMinimumRule() {
		return minimumRule;
	}
	public void setMinimumRule(boolean minimumRule) {
		this.minimumRule = minimumRule;
	}
	public boolean isMaximumRule() {
		return maximumRule;
	}
	public void setMaximumRule(boolean maximumRule) {
		this.maximumRule = maximumRule;
	}
	public boolean isMeanRule() {
		return meanRule;
	}
	public void setMeanRule(boolean meanRule) {
		this.meanRule = meanRule;
	}
	public boolean isModeRule() {
		return modeRule;
	}
	public void setModeRule(boolean modeRule) {
		this.modeRule = modeRule;
	}
	public String getKeyType() {
		return keyType;
	}
	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}
	public ArrayList<String> getKeyColumnNames() {
		return keyColumnNames;
	}
	public void setKeyColumnNames(ArrayList<String> keyColumnNames) {
		this.keyColumnNames = keyColumnNames;
	}
	public ConnectionDetailsBean getConnectionDetailsBean() {
		return connectionDetailsBean;
	}
	public void setConnectionDetailsBean(ConnectionDetailsBean connectionDetailsBean) {
		this.connectionDetailsBean = connectionDetailsBean;
	}
	

}
