package com.java.main.comparisons;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

public class MissingRows {

	public  List<Row> getMissingRows(JavaRDD<Row> tableRDD1,
			JavaRDD<Row> tableRDD2) {

		return tableRDD1.subtract(tableRDD2).collect();

	}

}
