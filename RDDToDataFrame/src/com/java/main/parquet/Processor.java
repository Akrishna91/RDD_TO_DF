package com.java.main.parquet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConversions;

import com.java.main.beans.ColSummary;
import com.java.main.beans.FinalSummaryBean;
import com.java.main.businessrules.DistinctValueBusinessRulesImpl;
import com.java.main.businessrules.PossibleValuesBusinessRules;
import com.java.main.constants.AggregationFuncNames;
import com.java.main.constants.ValueSeparater;
import com.java.main.context.GetJavaSparkContext;
import com.java.main.interfaces.IBusinessRule;
import com.java.main.processor.MainDriver;
import com.java.main.profiling.Profiling;
import com.java.main.ui.ConfigurationDetailsBean;
import com.java.main.utils.RDDUtils;

public class Processor {
	private static LinkedHashMap<String, LinkedHashSet<String>> possibleValues;
	private static ArrayList<String> distinctValueCols;

	public static FinalSummaryBean main(ConfigurationDetailsBean cdb)
			throws NoSuchAlgorithmException, IOException {
		JavaSparkContext jsc = GetJavaSparkContext.getJavaSparkContex();
		org.apache.spark.sql.SQLContext sqlContext = new org.apache.spark.sql.SQLContext(
				jsc);
		FinalSummaryBean finalSummaryBean = new FinalSummaryBean();
		String srcPath = cdb.getConnectionDetailsBean()
				.getSrcFileAbsolutePath();
		String destPath = cdb.getConnectionDetailsBean()
				.getDestFileAbsolutePath();

		DataFrame srcDataFrame = sqlContext.parquetFile(srcPath);
		DataFrame destDataFrame = sqlContext.parquetFile(destPath);
		// DataFrame destDataFrame = sqlContext.createDataFrame(destRDD,
		// destSchema);
		String[] colNames = new String[cdb.getColumnNames().size()];
		for (int i = 0; i < cdb.getColumnNames().size(); i++) {
			colNames[i] = cdb.getColumnNames().get(i);
		}
		// srcDataFrame.stat().freqItems(colNames).show();;
		System.out
				.println("We would be generating below rules and apply those rules on destination");
		System.out
				.println(" 1. Uniqueness Rules\n 2. Possible Value Rules\n 3. Association Rules\n 4. Mathematical Statistics");

		// Generating Rules on Source File
		// System.out.println("Enter Randomization weight. Please Enter between 0.1 to 1.0");
		// double randomPercent = new Scanner(System.in).nextDouble();
		// Sample the RDD from source

		// Apply these rules and Aggregation rules on destination

		if (cdb.isPossibleValueRule() || cdb.isUniquenessRule()) {
			generateRules(srcDataFrame, cdb.getColumnNames(),
					JavaConversions.seqAsJavaList(srcDataFrame.schema()
							.toList()), cdb.isPossibleValueRule(),
					cdb.isUniquenessRule());

		}
		List<AggregationFuncNames> types = new ArrayList<>();
		if (cdb.isSummationRule()) {
			types.add(AggregationFuncNames.SUM);
		}
		if (cdb.isMeanRule()) {
			types.add(AggregationFuncNames.AVG);
		}
		if (cdb.isMinimumRule()) {
			types.add(AggregationFuncNames.MIN);
		}
		if (cdb.isMaximumRule()) {
			types.add(AggregationFuncNames.MAX);
		}
		System.out.println(srcDataFrame.count()
				+ "-----------------------------------------------------");
		finalSummaryBean = MainDriver.main(srcDataFrame.javaRDD(),
				destDataFrame.javaRDD(), srcDataFrame, types,
				srcDataFrame.schema(), destDataFrame.schema(),
				cdb.getColumnNames(), cdb.getKeyColumnNames(), possibleValues,
				distinctValueCols);
		finalSummaryBean.setFinalStatus(false);

		return finalSummaryBean;

	}

	/**
	 * 
	 * @param file
	 * @return colNames and their data types
	 * @throws FileNotFoundException
	 */
	public HashMap<String, String> getColumnsDetails(String file)
			throws FileNotFoundException {
		HashMap<String, String> colNamesTypes = new LinkedHashMap<String, String>();
		JavaSparkContext jsc = GetJavaSparkContext.getJavaSparkContex();
		org.apache.spark.sql.SQLContext sqlContext = new org.apache.spark.sql.SQLContext(
				jsc);
		StructField[] fields = sqlContext.parquetFile(file).schema().fields();
		long startTime = System.currentTimeMillis();
		DataFrame table1 = sqlContext.parquetFile(file);
		table1.registerTempTable("table1");
		for (StructField field : fields) {
			colNamesTypes.put(field.name(), field.dataType().toString());
			String query = "SELECT SUM(" + field.name() + ") as SUM, AVG("
					+ field.name() + ") as AVG, MIN(" + field.name()
					+ ") as MIN, MAX(" + field.name() + ") as MAX from table1";
			try {
				DataFrame df = sqlContext.sql(query);
				df.collectAsList();
				System.out.println("list");
			} catch (Exception e) {
				System.out.println(field.name() + " "
						+ field.dataType().toString());
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println(endTime - startTime);
		return colNamesTypes;
	}
	
	public HashMap<String, String> compare(String file1, String file2)
			throws FileNotFoundException {
		JavaSparkContext jsc = GetJavaSparkContext.getJavaSparkContex();
		org.apache.spark.sql.SQLContext sqlContext = new org.apache.spark.sql.SQLContext(
				jsc);
		HashMap<String, String> colNamesTypes = new LinkedHashMap<String, String>();
		long startTime = System.currentTimeMillis();
		StructField[] fields = sqlContext.parquetFile(file1).schema().fields();
		DataFrame table1 = sqlContext.parquetFile(file1);
		DataFrame table2 = sqlContext.parquetFile(file2);
		table1.toJavaRDD().subtract(table2.toJavaRDD()).first();
		table1.registerTempTable("table1");
		table2.registerTempTable("table2");
		/*for (StructField field : fields) {
			String query = "SELECT SUM(" + field.name() + ") as SUM, AVG("
					+ field.name() + ") as AVG, MIN(" + field.name()
					+ ") as MIN, MAX(" + field.name() + ") as MAX from ";
			String q1 = query + "table1";
			String q2 = query + "table2";
			try {
				DataFrame df1 = sqlContext.sql(q1);
				DataFrame df2 = sqlContext.sql(q1);
				df1.except(df2).show();
				//df1.toJavaRDD()
				//df1.filter(df1.select("SUM") != df2.select("SUM")).show();
			} catch (Exception e) {
				System.out.println(field.name() + " "
						+ field.dataType().toString());
			}
		}*/
		long endTime = System.currentTimeMillis();
		System.out.println(endTime - startTime);
		return colNamesTypes;
	}

	/**
	 * sets the column involved in particular type of rule
	 * 
	 * @param dataFrame
	 * @param headers
	 * @param schema
	 * @param isPossibleValue
	 * @param isUniqueValue
	 * @return hashmap of rule type and corresponding rules
	 * 
	 */
	private static HashMap<String, ArrayList<String>> generateRules(
			DataFrame dataFrame, ArrayList<String> headers,
			List<StructField> schema, boolean isPossibleValue,
			boolean isUniqueValue) {
		HashMap<String, ArrayList<String>> rules = new LinkedHashMap<String, ArrayList<String>>();
		HashMap<String, HashMap<String, String>> summary = new LinkedHashMap<String, HashMap<String, String>>();
		// String[] colDataType = new String[headers.size()];

		Profiling profiling = new Profiling();
		HashSet<String> colNames = new HashSet<String>(headers);

		LinkedHashMap<String, ColSummary> colSummary = profiling.profiling(
				dataFrame, schema, headers);
		ArrayList<IBusinessRule> ruleGenrators = new ArrayList<IBusinessRule>();
		IBusinessRule pbr = null;
		IBusinessRule dbr = null;
		if (isUniqueValue) {
			dbr = new DistinctValueBusinessRulesImpl();
			ruleGenrators.add(dbr);
		}
		if (isPossibleValue) {
			pbr = new PossibleValuesBusinessRules();
			ruleGenrators.add(pbr);
		}

		for (String col : colSummary.keySet()) {
			if (colNames.contains(col)) {
				summary.put(col, colSummary.get(col).getSummary());
			}
		}

		for (IBusinessRule ruleGenrator : ruleGenrators) {
			ruleGenrator.genrateRule(colSummary, (int) dataFrame.count());
			rules.put(ruleGenrator.getClass().getSimpleName(),
					ruleGenrator.getFilteredRules(colNames));
		}
		if (pbr != null) {
			setPossibleValues(((PossibleValuesBusinessRules) pbr)
					.getPossibleValues());
		}
		if (dbr != null) {
			setDistinctValueCols(((DistinctValueBusinessRulesImpl) dbr)
					.getDistinctValueHolders());
		}

		System.out.println(rules);
		return rules;
	}

	public LinkedHashMap<String, LinkedHashSet<String>> getPossibleValues() {
		return possibleValues;
	}

	public static void setPossibleValues(
			LinkedHashMap<String, LinkedHashSet<String>> possibleValues1) {
		possibleValues = possibleValues1;
	}

	public ArrayList<String> getDistinctValueCols() {
		return distinctValueCols;
	}

	public static void setDistinctValueCols(ArrayList<String> distinctValueCols1) {
		distinctValueCols = distinctValueCols1;
	}

}
