package com.java.main.comparisons;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.mean;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.DataFrame;

import com.java.main.constants.AggregationFuncNames;

public class AggregationComparisonsHelper {
	
	/**
	 * given two tables, this method is used to apply aggregation comparisons on
	 * given columns
	 * 
	 * @param dataframe
	 *            table1
	 * @param dataframe
	 *            table2
	 * @param AggregationFuncNames
	 *            type
	 * @param String
	 *            colName
	 * @return
	 */
	public AggregationFuncNames compare(DataFrame table1, DataFrame table2,
			AggregationFuncNames type, String colName) {
		switch (type) {
		case SUM:
			if (!sumComparison(table1, table2, colName)) {
				return AggregationFuncNames.SUM;
			} else {
				return null;
			}
		case AVG:
			if (!meanComparison(table1, table2, colName)) {
				return AggregationFuncNames.AVG;
			} else {
				return null;
			}
		case MIN:
			if (!minComparison(table1, table2, colName)) {
				return AggregationFuncNames.MIN;
			} else {
				return null;
			}
		case MAX:
			if (!maxComparison(table1, table2, colName)) {
				return AggregationFuncNames.MAX;
			} else {
				return null;
			}
		default:
			break;
		}
		return null;

	}

	public boolean sumComparison(DataFrame table1, DataFrame table2,
			String colName) {
		try {
			return table1
					.select(sum(colName))
					.collectAsList()
					.get(0)
					.getAs(0)
					.equals(table2.select(sum(colName)).collectAsList().get(0)
							.getAs(0));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	public boolean meanComparison(DataFrame table1, DataFrame table2,
			String colName) {
		try {
			return table1
					.select(mean(colName))
					.collectAsList()
					.get(0)
					.getAs(0)
					.equals(table2.select(mean(colName)).collectAsList().get(0)
							.getAs(0));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	public boolean minComparison(DataFrame table1, DataFrame table2,
			String colName) {
		try {
			return table1
					.select(min(colName))
					.collectAsList()
					.get(0)
					.getAs(0)
					.equals(table2.select(min(colName)).collectAsList().get(0)
							.getAs(0));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	public boolean maxComparison(DataFrame table1, DataFrame table2,
			String colName) {
		try {
			return table1
					.select(max(colName))
					.collectAsList()
					.get(0)
					.getAs(0)
					.equals(table2.select(max(colName)).collectAsList().get(0)
							.getAs(0));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

}
