package com.java.main.interfaces;

import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.java.main.beans.AggregationMismatches;
import com.java.main.constants.AggregationFuncNames;
public interface IAggregationComparison {

	/**
	 * Returns comparison and the mismatches
	 * 
	 * @param DataFrame
	 *            table1
	 * @param DataFrame
	 *            table2
	 * @param array
	 *            of AggregationFuncNames type
	 * @param String
	 *            array of colNames
	 * @param sqlContext       
	 * @return boolean based on the comparison results
	 */
	public HashMap<String, List<AggregationMismatches>> compare(DataFrame table1, DataFrame table2,
			List<AggregationFuncNames> type, List<String> colNames, SQLContext sqlContext);

}
