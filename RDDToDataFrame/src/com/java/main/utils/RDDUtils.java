package com.java.main.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import com.java.main.constants.ValueSeparater;

public class RDDUtils {

	/**
	 * Returns the object function to remove the header from a RDD
	 * 
	 * @param srcFile
	 * @return file RDD without headers
	 */
	public static JavaRDD<String> removeHeaders(JavaRDD<String> srcFile) {
		Function2<Integer, Iterator<String>, Iterator<String>> rowsWithoutHeader = new Function2<Integer, Iterator<String>, Iterator<String>>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -6206662062838861709L;

			@Override
			public Iterator<String> call(Integer ind, Iterator<String> iterator)
					throws Exception {
				if (ind == 0 && iterator.hasNext()) {
					iterator.next();
					return iterator;
				} else
					return iterator;
			}
		};

		return srcFile.mapPartitionsWithIndex(rowsWithoutHeader, false);
	}

	/**
	 * 
	 * @param String
	 *            schemaString (column Names) comma saparated
	 * @param dataType
	 *            (array of datatypes)
	 * @return
	 */
	public static List<StructField> genrateSchema(String[] schemaString,
			ArrayList<DataType> dataType) {
		List<StructField> fields = new ArrayList<StructField>();
		int i = 0;
		System.out.println(dataType);
		for (String fieldName : schemaString) {
			fields.add(DataTypes.createStructField(fieldName.trim(),
					dataType.get(i), true));
			i++;
		}

		return fields;

	}

	/**
	 * 
	 * @param sample
	 * @param fields
	 * @return
	 */
	public static JavaRDD<Row> genrateJavaRDD(JavaRDD<String> sample,
			final List<StructField> fields) {
		final HashMap<String, String> dateFormates = getDateFormate(fields,
				sample.first().trim().split(","));
		JavaRDD<Row> rowRDD = sample.map(new Function<String, Row>() {
			/**
					 * 
					 */
			private static final long serialVersionUID = 1L;

			public Row call(String record) throws Exception {
				String[] lineString = record.split(ValueSeparater.getRegex());
				Object[] cells = new Object[fields.size()];
				for (int i = 0; i < fields.size(); i++) {
					// System.out.println(fields.get(i));
					if (fields.get(i).dataType().equals(DataTypes.DoubleType)) {
						try {
							cells[i] = Double.parseDouble(lineString[i].trim());
						} catch (Exception e) {
							cells[i] = Double.parseDouble("0");
						}
					} else if (fields.get(i).dataType()
							.equals(DataTypes.DateType)) {
						try {
							String fieldName = fields.get(i).name();
							cells[i] = new java.sql.Date(DateUtils.parse(
									lineString[i].trim(),
									dateFormates.get(fieldName)).getTime());
						} catch (Exception e) {
							cells[i] = new java.sql.Date(DateUtils.parse(
									"01-01-0001").getTime());
						}
					} else {

						try {
							if (lineString[i].trim().length() != 0) {
								cells[i] = lineString[i].trim();
							} else {
								cells[i] = "Na";
							}
						} catch (Exception e) {
							cells[i] = "Na";
						}
					}
				}
				return RowFactory.create(cells);
			}
		});

		return rowRDD;

	}

	/**
	 * To generate DataTypes using the first row of the data
	 * 
	 * @param firstRow
	 *            (CSV String value)
	 * @return
	 */
	public static ArrayList<DataType> genrateDataTypes(String firstRow) {

		ArrayList<DataType> dataType = new ArrayList<DataType>();
		String[] firstRowArray = firstRow.trim().split(
				ValueSeparater.getRegex(), -1);

		for (String cell : firstRowArray) {
			try {
				if (DataTypeUtils.isDouble(cell.trim())) {
					dataType.add(DataTypes.DoubleType);
				} else if (DateUtils.isValidDate(cell.trim())) {
					dataType.add(DataTypes.DateType);
				} else {
					dataType.add(DataTypes.StringType);
				}
			} catch (NullPointerException ex) {
				// if null then keep the data type as string
				dataType.add(DataTypes.StringType);
				System.out.println(" Null found in resolving data type");
			}

		}
		return dataType;
	}

	
	/**
	 * 
	 * @param fields
	 * @param data
	 *            (any non null row)
	 * @return colname and its date format as hashmap
	 */
	public static HashMap<String, String> getDateFormate(
			final List<StructField> fields, String[] data) {

		HashMap<String, String> dateFormates = new HashMap<>();
		for (int i = 0; i < fields.size(); i++) {
			if (fields.get(i).dataType().equals(DataTypes.DateType)) {
				try {
					String dateFormate = DateUtils.determineDateFormat(data[i]);
					dateFormates.put(fields.get(i).name(), dateFormate);

				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}
		return dateFormates;
	}

}
