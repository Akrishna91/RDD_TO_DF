package com.java.main.businessrules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import com.java.main.beans.ColSummary;
import com.java.main.interfaces.IBusinessRule;


/**
 * Class responsible for generating business rules based on possible values of
 * attribute
 * 
 * @author Azad
 *
 */
public class PossibleValuesBusinessRules implements IBusinessRule {

	private LinkedHashMap<String, LinkedHashSet<String>> possibleValues;

	/**
	 * 
	 * @param ArrayList
	 *            of LinkedHashSet levels
	 * @param cols
	 */
	@Override
	public void genrateRule(LinkedHashMap<String, ColSummary> colSummary,
			int numRows) {

		LinkedHashMap<String, LinkedHashSet<String>> possibleValues = new LinkedHashMap<String, LinkedHashSet<String>>();
		for (String col : colSummary.keySet()) {
			int cardinality = colSummary.get(col).getCardinality();
			if (cardinality <= 10 && cardinality > 0) {
				possibleValues.put(col, colSummary.get(col).getLevels());
			}
		}
		setPossibleValues(possibleValues);
		//print(possibleValues);
	}

	@Override
	public ArrayList<String> getRules() {
		ArrayList<String> rules = new ArrayList<String>();
		for (String col : possibleValues.keySet()) {
			rules.add(col + " has " + possibleValues.get(col).size()
					+ " possible values " + possibleValues.get(col));
		}
		return rules;
	}

	@Override
	public ArrayList<String> getFilteredRules(HashSet<String> colNames) {
		ArrayList<String> rules = new ArrayList<String>();
		for (String col : possibleValues.keySet()) {
			if (colNames.contains(col)) {
				rules.add(col + " has " + possibleValues.get(col).size()
						+ " possible values " + possibleValues.get(col));
			}
		}
		return rules;
	}

	public String toString() {
		String rules = "";
		for (String col : possibleValues.keySet()) {
			rules += col + ": " + possibleValues.get(col) + "\n";
		}
		return rules;
	}

	public void print(
			LinkedHashMap<String, LinkedHashSet<String>> possibleValues) {

		System.out
				.println("----------------Possible Values Rule----------------");
		for (String col : possibleValues.keySet()) {
			System.out.println(col + ": " + possibleValues.get(col));
		}
	}

	public LinkedHashMap<String, LinkedHashSet<String>> getPossibleValues() {
		return possibleValues;
	}

	public void setPossibleValues(
			LinkedHashMap<String, LinkedHashSet<String>> possibleValues) {
		this.possibleValues = possibleValues;
	}

	

}
