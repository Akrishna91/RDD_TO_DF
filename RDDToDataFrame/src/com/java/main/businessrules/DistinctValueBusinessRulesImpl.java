package com.java.main.businessrules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;

import com.java.main.beans.ColSummary;
import com.java.main.interfaces.IBusinessRule;


/**
 * Class which is responsible for generating business rules based on distinct
 * values on each attribute
 * 
 * @author Karthik
 *
 */
public class DistinctValueBusinessRulesImpl implements IBusinessRule {
	private ArrayList<String> distinctValueHolders;
	
	@Override
	public void genrateRule(LinkedHashMap<String, ColSummary> colSummary,
			int numRows) {
		ArrayList<String> distinctValueHolders = new ArrayList<String>();
		for (String col : colSummary.keySet()) {
			int cardinality = colSummary.get(col).getCardinality();
			if (cardinality >= 0.98 * numRows && cardinality <= 1.02 * numRows) {
				distinctValueHolders.add(col);
			}
		}
		setDistinctValueHolders(distinctValueHolders);
		//System.out
		//		.println("------------------- Uniqueness Rules --------------");
		//System.out.println(distinctValueHolders);
	}

	@Override
	public ArrayList<String> getRules() {
		ArrayList<String> rules = new ArrayList<String>();
		for (String col : distinctValueHolders) {
			rules.add(col + " should have only unique values.");
		}
		return rules;
	}

	@Override
	public ArrayList<String> getFilteredRules(HashSet<String> colNames) {
		ArrayList<String> rules = new ArrayList<String>();
		for (String col : distinctValueHolders) {
			if (colNames.contains(col)) {
				rules.add(col + " should have only unique values.");
			}
		}
		return rules;
	}

	public String toString() {
		String rules = "";
		for (String col : distinctValueHolders) {
			rules += col + "\n";
		}
		return rules;
	}

	public ArrayList<String> getDistinctValueHolders() {
		return distinctValueHolders;
	}

	public void setDistinctValueHolders(ArrayList<String> distinctValueHolders) {
		this.distinctValueHolders = distinctValueHolders;
	}

}
