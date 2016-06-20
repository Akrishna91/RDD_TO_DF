package com.java.main.profiling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import com.java.main.beans.ColSummary;
import com.java.main.utils.DataFrameUtils;

import static org.apache.spark.sql.functions.*;

public class Profiling {

	public LinkedHashMap<String, ColSummary> profiling(DataFrame dataFrame,
			List<StructField> srcSchema, ArrayList<String> selectedCols) {
		// dataFrame.describe().show();
		System.out.println("Profiling Started");
		LinkedHashMap<String, ColSummary> colSummary = new LinkedHashMap<String, ColSummary>();
		HashSet<String> selectedColSet = new HashSet<String>();
		for (String selectedCol : selectedCols) {
			selectedColSet.add(selectedCol);
		}
		for (StructField field : srcSchema) {
			ColSummary currentColSummary = new ColSummary();
			String colName = field.name().trim();
			if (selectedColSet.contains(colName.trim())) {
				System.out.println(colName);
				LinkedHashSet<String> levels = null;
				long cardinality = DataFrameUtils.getCardinality(dataFrame,
						colName);
				currentColSummary.setCardinality((int) cardinality);
				if (cardinality <= 15) {
					levels = DataFrameUtils.getDistinctValues(dataFrame,
							colName);
					currentColSummary.setLevels(levels);
				}

				if (field.dataType().equals(DataTypes.StringType)) {
					currentColSummary.setType("String");
				} else if (field.dataType().equals(DataTypes.DateType)){
					currentColSummary.setType("Date");
					currentColSummary.setFormats(DataFrameUtils.getDateFormats(dataFrame, colName));
					
				}else {
					currentColSummary.setType("Numeric");
					if (levels != null) {
						currentColSummary.setMax(Double.parseDouble(Collections
								.max(levels)));
						currentColSummary.setMin(Double.parseDouble(Collections
								.min(levels)));

					} else {
						Row[] max_min = dataFrame.select(max(colName),
								min(colName)).collect();
						currentColSummary.setMax(Double.parseDouble(max_min[0]
								.getAs(0).toString()));
						currentColSummary.setMin(Double.parseDouble(max_min[0]
								.getAs(1).toString()));
					}
				}
				currentColSummary.setLevels(levels);
				colSummary.put(colName, currentColSummary);
			}

		}
		return colSummary;

	}

	public LinkedHashMap<String, ColSummary> profiling(DataFrame dataFrame,
			List<StructField> srcSchema) {
		// dataFrame.describe().show();
		System.out.println("Profiling Started");
		LinkedHashMap<String, ColSummary> colSummary = new LinkedHashMap<String, ColSummary>();
		for (StructField field : srcSchema) {
			ColSummary currentColSummary = new ColSummary();
			String colName = field.name();
			System.out.println(colName);
			LinkedHashSet<String> levels = null;
			long cardinality = DataFrameUtils
					.getCardinality(dataFrame, colName);
			currentColSummary.setCardinality((int) cardinality);
			if (cardinality <= 15) {
				levels = DataFrameUtils.getDistinctValues(dataFrame, colName);
				currentColSummary.setLevels(levels);
			}

			if (field.dataType().equals(DataTypes.StringType)) {
				currentColSummary.setType("String");
			} else {
				currentColSummary.setType("Numeric");
				if (levels != null) {
					currentColSummary.setMax(Double.parseDouble(Collections
							.max(levels)));
					currentColSummary.setMin(Double.parseDouble(Collections
							.min(levels)));

				} else {
					Row[] max_min = dataFrame
							.select(max(colName), min(colName)).collect();
					currentColSummary.setMax(Double.parseDouble(max_min[0]
							.getAs(0).toString()));
					currentColSummary.setMin(Double.parseDouble(max_min[0]
							.getAs(1).toString()));
				}
			}
			currentColSummary.setLevels(levels);
			colSummary.put(colName, currentColSummary);
		}
		return colSummary;

	}
}
