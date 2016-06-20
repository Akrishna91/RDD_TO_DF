package com.java.main.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.java.main.beans.FinalSummaryBean;
import com.java.main.constants.AggregationFuncNames;

public class MainDriver {

	public static FinalSummaryBean main(JavaRDD<Row> srcRowRDD, JavaRDD<Row> destRowRDD, DataFrame srcDataFrame,
			List<AggregationFuncNames> types, StructType sourceSchema,
			StructType destSchema,List<String> colNames, ArrayList<String> keyColumnNames,
			LinkedHashMap<String, LinkedHashSet<String>> possibleValues,
			ArrayList<String> distinctValueCols) {
		/*
		 * String path1 = "/home/cloudera/Desktop/song_data.csv"; String path2 =
		 * "/home/cloudera/Desktop/song_data_copy.csv";
		 */
		
		
		Arrays.asList(AggregationFuncNames.values());
		return DynamicSchema.main(srcRowRDD, destRowRDD, srcDataFrame, sourceSchema,
				destSchema, colNames, keyColumnNames, types, possibleValues, distinctValueCols);

	}

}
