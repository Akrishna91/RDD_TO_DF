package com.java.main.utils;

import java.util.LinkedHashSet;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

public class DataFrameUtils {

	public static LinkedHashSet<String> getDistinctValues(DataFrame dataFrame,
			String colName) {

		Row[] distinctValuesRow = dataFrame.select(colName).distinct()
				.collect();

		LinkedHashSet<String> distingValues = new LinkedHashSet<String>();
		for (int i = 0; i < distinctValuesRow.length; i++) {
			try {
				distingValues.add(distinctValuesRow[i].getAs(0).toString());
			} catch (NullPointerException e) {
				distingValues.add("null");
			}

		}
		return distingValues;

	}

	public static LinkedHashSet<String> getDistinctValues(DataFrame dataFrame) {
		Row[] distinctValuesRow = dataFrame.distinct().collect();
		LinkedHashSet<String> distingValues = new LinkedHashSet<String>();
		for (int i = 0; i < distinctValuesRow.length; i++) {
			try {
				distingValues.add(distinctValuesRow[i].getAs(0).toString());
			} catch (NullPointerException e) {
				distingValues.add("null");
			}

		}
		return distingValues;

	}

	public static Long getCardinality(DataFrame df, String colName) {
		/*
		 * df.registerTempTable("table"); SQLContext sqlContext = new
		 * org.apache.spark.sql.SQLContext(
		 * GetJavaSparkContext.getJavaSparkContex()); String query =
		 * "SELECT COUNT(DISTINCT "; query += colName; query += ") from table";
		 * DataFrame count = sqlContext.sql(query); count.show();
		 */
		// DataFrame count = df.select(countDistinct(colName));
		// count.show();
		// return count.collect()[0].getLong(0);
		return df.select(colName).distinct().count();
	}

	public static LinkedHashSet<String> getDateFormats(DataFrame dataFrame,
			String colName) {
		LinkedHashSet<String> dateFormates = new LinkedHashSet<String>();
		try {
			String firstDate = dataFrame.select(colName).head().getAs(0)
					.toString();
			dateFormates.add(DateUtils.determineDateFormat(firstDate));
			return dateFormates;
		} catch (Exception e) {
			return dateFormates;
		}

	}
}
