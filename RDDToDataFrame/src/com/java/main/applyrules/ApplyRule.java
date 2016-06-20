package com.java.main.applyrules;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.java.main.beans.RulesComparaorResult;
import com.java.main.constants.RulesMatchingStatus;
import com.java.main.utils.DataFrameUtils;

public class ApplyRule {

	public LinkedHashMap<String, RulesComparaorResult> possibleValueCompare(
			LinkedHashMap<String, LinkedHashSet<String>> sourceRule,
			DataFrame destDataFrame) {
		LinkedHashMap<String, RulesComparaorResult> result = new LinkedHashMap<String, RulesComparaorResult>();
		for(String colName: sourceRule.keySet()){
			System.out.println(colName);
		}
		for (String colName : sourceRule.keySet()) {
			RulesComparaorResult rcr = new RulesComparaorResult();
			LinkedHashSet<String> possibleSourceValues = sourceRule
					.get(colName);
		
			LinkedHashSet<String> possibleDestValues = DataFrameUtils.getDistinctValues(destDataFrame
					.select(colName));
			rcr.setSourceValues(possibleSourceValues);
			rcr.setDestValues(possibleDestValues);

			if (possibleSourceValues.size() == possibleDestValues.size()) {
				boolean statusSet = false;
				for (String possibleSourceValue : possibleSourceValues) {
					if (!possibleDestValues.contains(possibleSourceValue)) {
						rcr.setStatus(RulesMatchingStatus.MIGHT_MATCH);
						statusSet = true;
						break;
					}
				}
				if (!statusSet) {
					rcr.setStatus(RulesMatchingStatus.MATCHED);
				}
			} else {
				rcr.setStatus(RulesMatchingStatus.MISMATCHED);
			}
			result.put(colName, rcr);
		}

		return result;
	}

	public LinkedHashMap<String, RulesComparaorResult> distinctValueCompare(
			ArrayList<String> distinctValueCols, DataFrame destDataFrame) {
		LinkedHashMap<String, RulesComparaorResult> result = new LinkedHashMap<String, RulesComparaorResult>();
		for(String distinctValueCol: distinctValueCols){
			RulesComparaorResult rcr = new RulesComparaorResult();
			LinkedHashSet<String> sourceCol  = new LinkedHashSet<String>();
			LinkedHashSet<String> destCol  = new LinkedHashSet<String>();
			sourceCol.add("unique");
			rcr.setSourceValues(sourceCol);
			if (isUniqueColumn(destDataFrame.select(distinctValueCol))){
				rcr.setStatus(RulesMatchingStatus.MATCHED);
				destCol.add("unique");
			}else{
				rcr.setStatus(RulesMatchingStatus.MISMATCHED);
				destCol.add("not Unique");
			}
			rcr.setDestValues(destCol);
			result.put(distinctValueCol, rcr);
		}
		return result;

	}

	public LinkedHashSet<String> getDistinctValues(DataFrame dataFrame,
			String colName) {

		Row[] distinctValuesRow = dataFrame.select(colName).distinct().collect();
		
		LinkedHashSet<String> distingValues = new LinkedHashSet<String>();
		for (int i = 0; i < distinctValuesRow.length; i++) {
			distingValues.add(distinctValuesRow[i].getAs(i).toString().toUpperCase());
		}
		return distingValues;

	}

	

	public boolean isUniqueColumn(DataFrame dataFrame, String colName) {
		Long distinctValuesCount = dataFrame.select(colName).distinct().count();
		Long numRows = dataFrame.count();
		return distinctValuesCount.equals(numRows);
	}

	public boolean isUniqueColumn(DataFrame dataFrame) {
		Long distinctValuesCount = dataFrame.distinct().count();
		Long numRows = dataFrame.count();
		return distinctValuesCount.equals(numRows);
	}

}
