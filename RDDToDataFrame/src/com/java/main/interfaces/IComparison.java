package com.java.main.interfaces;

import org.apache.spark.sql.DataFrame;

import com.java.main.constants.Status;

public interface IComparison {
	/**
	 * 
	 * Returns comparison and the mismatches
	 * @param DataFrame table from spark sql
	 * @return
	 */
	public Status compare(DataFrame table1, DataFrame table2);

}
