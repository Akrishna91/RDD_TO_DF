package com.java.main.comparisons;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.java.main.beans.AggregationMismatches;
import com.java.main.constants.AggregationFuncNames;
import com.java.main.interfaces.IAggregationComparison;

public class AggregationComparison implements IAggregationComparison {

	@Override
	public HashMap<String, List<AggregationMismatches>> compare(
			DataFrame table1, DataFrame table2,
			List<AggregationFuncNames> types, List<String> colNames,
			SQLContext sqlContext) {

		HashMap<String, List<AggregationMismatches>> colNames_AggError = new HashMap<String, List<AggregationMismatches>>();
		//AggregationComparisonsHelper ach = new AggregationComparisonsHelper();
		for (String colName : colNames) {
			List<AggregationMismatches> aggregationMismatches = new ArrayList<AggregationMismatches>();
			// Register the DataFrame as a table.
			table1.registerTempTable("table1");
			table2.registerTempTable("table2");
			String query1 = aggregationQueryGenrator("table1", colName, types);
			String query2 = aggregationQueryGenrator("table2", colName, types);
			Row queryResult1 = executeQuery(query1, sqlContext);
			Row queryResult2 = executeQuery(query2, sqlContext);
			try {
				ArrayList<Integer> misMatchesIndexes = compareResults(
						queryResult1, queryResult2);
				for (Integer index : misMatchesIndexes) {
					AggregationMismatches aggregationMismatch = new AggregationMismatches();
					aggregationMismatch.setSourceValue(queryResult1.getAs(index).toString());
					aggregationMismatch.setDestValue(queryResult2.getAs(index).toString());
					aggregationMismatch.setMisMatchedFuncName(types.get(index));
					aggregationMismatches.add(aggregationMismatch);
				}
				
			} catch (NullPointerException e) {
				AggregationMismatches aggregationMismatch = new AggregationMismatches();
				aggregationMismatch.setMisMatchedFuncName((AggregationFuncNames.ERROR));
				aggregationMismatches.add(aggregationMismatch);
			}
			if (aggregationMismatches.size() > 0) {
				colNames_AggError.put(colName, aggregationMismatches);
			}
			
		}
		System.out.println("-------------- Aggregation Comp Ends --------------");
		return colNames_AggError;
	}

	/**
	 * Generates a select query for the given aggregation functions
	 * 
	 * @param tableName
	 * @param colName
	 * @param types
	 * @return String query
	 */
	public String aggregationQueryGenrator(String tableName, String colName,
			List<AggregationFuncNames> types) {
		String query = "SELECT ";
		for (int i = 0; i < types.size(); i++) {
			query += types.get(i).name() + "(" + colName + ") as "
					+ types.get(i).name();
			if (i < types.size() - 1) {
				query += ", ";
			}
		}

		query += " from " + tableName;
		return query;
	}

	/**
	 * given a query, it executes the query and returns its result
	 * 
	 * @param query
	 * @param sqlContext
	 * @return query result
	 */
	public Row executeQuery(String query, SQLContext sqlContext) {
		//DataFrame result = sqlContext.sql(query);
		try{
			DataFrame result = sqlContext.sql(query);
			return result.collectAsList().get(0);
		}catch (Exception e){
			return null;
		}
		
		
	}

	/**
	 * Compares two result and returns the index where the mismatches
	 * 
	 * @param queryResult1
	 * @param queryResult2
	 * @return comparison result and mismatched indexes
	 * @throws NullPointerException
	 */
	public ArrayList<Integer> compareResults(Row queryResult1, Row queryResult2)
			throws NullPointerException {
		ArrayList<Integer> result = new ArrayList<Integer>();
		for (int i = 0; i < queryResult1.size(); i++) {
			if (!queryResult1.getAs(i).equals(queryResult2.getAs(i))) {
				result.add(i);
			}
		}
		return result;
	}

}
