package com.java.main.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.java.main.applyrules.ApplyRule;
import com.java.main.beans.AggregationMismatches;
import com.java.main.beans.ColResultSummaryBean;
import com.java.main.beans.FinalSummaryBean;
import com.java.main.beans.RulesComparaorResult;
import com.java.main.comparisons.AggregationComparison;
import com.java.main.comparisons.MissingRows;
import com.java.main.constants.AggregationFuncNames;
import com.java.main.constants.RulesMatchingStatus;
import com.java.main.context.GetJavaSparkContext;
import com.java.main.interfaces.IAggregationComparison;

public class DynamicSchema {
	/**
	 * 
	 * @param sampleSrcRowRDD
	 * @param destRowRDD
	 * @param srcDataFrame
	 * @param sourceSchema
	 * @param destSchema
	 * @param colNames
	 * @param keyColumnNames
	 * @param types
	 * @param possibleValues
	 * @param distinctValueCols
	 * @return bean containing final results
	 */
	public static FinalSummaryBean main(JavaRDD<Row> sampleSrcRowRDD,
			JavaRDD<Row> destRowRDD, DataFrame srcDataFrame,
			StructType sourceSchema, StructType destSchema,
			List<String> colNames, ArrayList<String> keyColumnNames,
			List<AggregationFuncNames> types,
			LinkedHashMap<String, LinkedHashSet<String>> possibleValues,
			ArrayList<String> distinctValueCols) {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);

		HashMap<String, ColResultSummaryBean> colResultSummaryBeans = new HashMap<String, ColResultSummaryBean>();
		FinalSummaryBean finalSummary = new FinalSummaryBean();
		org.apache.spark.sql.SQLContext sqlContext = new org.apache.spark.sql.SQLContext(
				GetJavaSparkContext.getJavaSparkContex());
		// sourceSchema.printTreeString();
		// Apply the schema to the RDD.
		DataFrame sampleSourceDataFrame = sqlContext.createDataFrame(
				sampleSrcRowRDD, sourceSchema);
		DataFrame destDataFrame = sqlContext.createDataFrame(destRowRDD,
				destSchema);
		// destDataFrame.printSchema();
		// Get the same data from the destination as sample source
		// MatchedRecords matchedRecords = new
		// MatchedRecords(sampleSourceDataFrame, destDataFrame, keyColumnNames,
		// sqlContext);
		// DataFrame sampleDestDataFrame = matchedRecords.fetMatchingRecords();

		// System.out.println(destDataFrame.schema());
		// destDataFrame = naHandler.fill("null");
		// destDataFrame = destDataFrame.na().fill("null");
		// destDataFrame.show();
		// Comparisons
		IAggregationComparison agc = new AggregationComparison();
		// Aggregation comparisons
		HashMap<String, List<AggregationMismatches>> colNames_AggError;
		colNames_AggError = agc.compare(sampleSourceDataFrame, destDataFrame,
				types, colNames, sqlContext);
		for (String colName : colNames_AggError.keySet()) {
			System.out.println(colName + ":");
			System.out.println("Errors:");
			for (AggregationMismatches error : colNames_AggError.get(colName)) {
				System.out.println(error.getMisMatchedFuncName() + "- ");
				System.out.println("Source Value:" + error.getSourceValue());
				System.out.println("Dest Value:" + error.getDestValue());
			}
			System.out.println();
		}

		// sourceDataFrame.except(destDataFrame).show();
		// Finding the mis System.out.println();
		System.out.println("**********Mis Matched Rows*************");
		if (colNames_AggError.size() > 0) {
			MissingRows mr = new MissingRows();
			List<Row> missingRows = mr.getMissingRows(sampleSrcRowRDD,
					destRowRDD);

			finalSummary.setMissingRows(missingRows);
			if (missingRows.size() != 0) {
				for (Row missingRow : missingRows) {
					System.out.println(missingRow);
				}
			}
		}

		// Apply Rules generated at source
		ApplyRule ar = new ApplyRule();
		LinkedHashMap<String, RulesComparaorResult> distinctRuleResults = null;
		LinkedHashMap<String, RulesComparaorResult> possibleValueRuleResults = null;
		if (distinctValueCols != null) {
			distinctRuleResults = ar.distinctValueCompare(distinctValueCols,
					destDataFrame);
			System.out
					.println("*******************Unique Columns Report**********************");
			for (String colName : distinctRuleResults.keySet()) {
				System.out.println("column name: " + colName);
				System.out.println(distinctRuleResults.get(colName));
			}
		}

		if (possibleValues != null) {
			possibleValueRuleResults = ar.possibleValueCompare(possibleValues,
					destDataFrame);
			System.out.println();
			System.out
					.println("*******************Possible Value Report**********************");
			for (String colName : possibleValueRuleResults.keySet()) {
				System.out.println("column name: " + colName);
				System.out.println(possibleValueRuleResults.get(colName));
			}
		}

		for (String col : colNames) {
			ColResultSummaryBean colResultSummaryBean = new ColResultSummaryBean();
			for (AggregationFuncNames type : types) {
				switch (type) {
				case SUM:
					colResultSummaryBean
							.setSummationRuleResult(RulesMatchingStatus.MATCHED);
					break;
				case AVG:
					colResultSummaryBean
							.setMeanRuleResult(RulesMatchingStatus.MATCHED);
					break;
				case MAX:
					colResultSummaryBean
							.setMaximumRuleResult(RulesMatchingStatus.MATCHED);
					break;
				case MIN:
					colResultSummaryBean
							.setMinimumRuleResult(RulesMatchingStatus.MATCHED);
					break;
				default:
					break;
				}
			}
			if (colNames_AggError.containsKey(col)) {
				List<AggregationMismatches> aggMisMatch = colNames_AggError
						.get(col);
				for (AggregationMismatches misMatch : aggMisMatch) {
					switch (misMatch.getMisMatchedFuncName()) {
					case SUM:
						colResultSummaryBean
								.setSummationRuleResult(RulesMatchingStatus.MISMATCHED);
						break;
					case AVG:
						colResultSummaryBean
								.setMeanRuleResult(RulesMatchingStatus.MISMATCHED);
						break;
					case MAX:
						colResultSummaryBean
								.setMaximumRuleResult(RulesMatchingStatus.MISMATCHED);
						break;
					case MIN:
						colResultSummaryBean
								.setMinimumRuleResult(RulesMatchingStatus.MISMATCHED);
						break;
					default:
						break;
					}
				}
			}

			if (distinctRuleResults != null
					&& distinctRuleResults.containsKey(col)) {
				RulesComparaorResult rulesComparaorResult = distinctRuleResults
						.get(col);
				colResultSummaryBean
						.setUniquenessRuleResult(rulesComparaorResult
								.getStatus());
			}
			if (possibleValueRuleResults != null
					&& possibleValueRuleResults.containsKey(col)) {
				RulesComparaorResult rulesComparaorResult = possibleValueRuleResults
						.get(col);
				colResultSummaryBean
						.setPossibleValueRuleResult(rulesComparaorResult
								.getStatus());
			}
			colResultSummaryBeans.put(col, colResultSummaryBean);

		}
		finalSummary.setColResultSummaryBean(colResultSummaryBeans);
		finalSummary.setColNames_AggError(colNames_AggError);
		finalSummary.setDistinctRuleResults(distinctRuleResults);
		finalSummary.setPossibleValueRuleResults(possibleValueRuleResults);
		System.out
				.println("-------------------done---------------------------");

		// Basic Comparision
		/*
		 * IComparison basicComparision = new BasicComparison();
		 * System.out.println(basicComparision.compare(sampleDataFrame1,
		 * sampleDataFrame2));
		 */

		// Register the DataFrame as a table.
		sampleSourceDataFrame.registerTempTable("sample1");
		destDataFrame.registerTempTable("sample2");
		// SQL can be run over RDDs that have been registered as tables.

		// DataFrame names =
		// sqlContext.sql("SELECT SUM(listen_count), AVG(listen_count), MIN(listen_count), MAX(listen_count) FROM sample1 ");
		// names.show();
		// DataFrame names =
		// sqlContext.sql("select name from sample2") ;
		// names.show();
		/*
		 * for (String col : sampleDataFrame1.columns()) {
		 * System.out.println(col); }
		 */
		// The results of SQL queries are DataFrames and support all the normal
		// RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		/*
		 * List<String> namesList = names.javaRDD() .map(new Function<Row,
		 * String>() {
		 *//**
			 * 
			 */
		/*
		 * private static final long serialVersionUID = -5552987624028457332L;
		 * 
		 * public String call(Row row) { return "Name: " + row.getInt(0); }
		 * }).collect(); System.out.println(namesList);
		 */
		// names.show();

		return finalSummary;
	}
}
