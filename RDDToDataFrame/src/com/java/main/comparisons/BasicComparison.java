package com.java.main.comparisons;

import org.apache.spark.sql.DataFrame;

import com.java.main.constants.Status;
import com.java.main.interfaces.IComparison;

/**
 * This class is responsible for basic comparisons such as number of rows and
 * columns, column names etc.
 * 
 * @author cloudera
 *
 */

public class BasicComparison implements IComparison {
	public Status compare(DataFrame table1, DataFrame table2) {
		if (!isRowMatched(table1.count(), table2.count())) {
			if (!isColMatched((long)table1.columns().length, (long)table2.columns().length)) {
				return Status.ROW_COL_MISMATCH;
			} else {
				return Status.ROW_MISMATCH;
			}
		} else if (!isColMatched((long)table1.columns().length, (long)table2.columns().length)) {
			return Status.COL_MISMATCH;
		} else {
			return Status.OK;
		}
	}

	/**
	 * checks if there is any mismatch in number of rows
	 * 
	 * @param int numRows1
	 * @param int numRows2
	 * @return
	 */
	public boolean isRowMatched(Long numRows1, Long numRows2) {
		return numRows1.equals(numRows2) ;

	}

	/**
	 * checks if there is any mismatch in number of cols
	 * 
	 * @param int numCols1
	 * @param int numCols2
	 * @return
	 */
	public boolean isColMatched(Long numCols1, Long numCols2) {
		return numCols1.equals(numCols2);

	}

}
