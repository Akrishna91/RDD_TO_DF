package com.java.main.beans;
import java.util.LinkedHashSet;

import com.java.main.constants.RulesMatchingStatus;

public class RulesComparaorResult {
	
	private RulesMatchingStatus status;
	private LinkedHashSet<String> sourceValues;
	private LinkedHashSet<String> destValues;
	
	public String toString(){
		String result = "";
		result += "status: " + status.name() + "\n";
		result += "source: " + sourceValues + "\n";
		result += "dest: " + destValues + "\n";
		return result;
	}
	public RulesMatchingStatus getStatus() {
		return status;
	}
	public void setStatus(RulesMatchingStatus status) {
		this.status = status;
	}
	public LinkedHashSet<String> getSourceValues() {
		return sourceValues;
	}
	public void setSourceValues(LinkedHashSet<String> sourceValues) {
		this.sourceValues = sourceValues;
	}
	public LinkedHashSet<String> getDestValues() {
		return destValues;
	}
	public void setDestValues(LinkedHashSet<String> destValues) {
		this.destValues = destValues;
	}
	

}
