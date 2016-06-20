package com.java.main.processor;

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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;

import com.java.main.beans.ColSummary;
import com.java.main.beans.FinalSummaryBean;
import com.java.main.businessrules.DistinctValueBusinessRulesImpl;
import com.java.main.businessrules.PossibleValuesBusinessRules;
import com.java.main.checksum.CheckSumGenerator;
import com.java.main.constants.AggregationFuncNames;
import com.java.main.constants.ValueSeparater;
import com.java.main.context.GetJavaSparkContext;
import com.java.main.interfaces.IBusinessRule;
import com.java.main.profiling.Profiling;
import com.java.main.ui.ConfigurationDetailsBean;
import com.java.main.utils.RDDUtils;

public class Processor {
	private static LinkedHashMap<String, LinkedHashSet<String>> possibleValues;
	private static ArrayList<String> distinctValueCols;
	
	public static FinalSummaryBean parqMain(ConfigurationDetailsBean cdb)
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
		
		// Apply these rules and Aggregation rules on destination

		if (cdb.isPossibleValueRule() || cdb.isUniquenessRule()) {
			generateRules(srcDataFrame, cdb.getColumnNames(),JavaConversions.seqAsJavaList(srcDataFrame.schema().toList()),
					cdb.isPossibleValueRule(), cdb.isUniquenessRule());

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
		finalSummaryBean = MainDriver.main(srcDataFrame.javaRDD(), destDataFrame.javaRDD(),
				srcDataFrame, types, srcDataFrame.schema(), destDataFrame.schema(),
				cdb.getColumnNames(), cdb.getKeyColumnNames(), possibleValues,
				distinctValueCols);
		finalSummaryBean.setFinalStatus(false);

		return finalSummaryBean;

	}
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
		JavaRDD<String> srcFile = jsc.textFile(srcPath);
		JavaRDD<String> destFile = jsc.textFile(destPath);

		String[] srcHeaders = srcFile.first().split(ValueSeparater.getRegex());
		String[] destHeaders = srcFile.first().split(ValueSeparater.getRegex());
		// 1st Step
		// MD5 Checksum
		System.out.println("Did MD5 Checksum");
		CheckSumGenerator srcCheckSum = new CheckSumGenerator("MD5", srcPath);
		System.out.println("Source File Checksum-->"
				+ srcCheckSum.generateChecksum());
		CheckSumGenerator destCheckSum = new CheckSumGenerator("MD5", destPath);
		System.out.println("Destination File Checksum-->"
				+ destCheckSum.generateChecksum());
		if (srcCheckSum.generateChecksum().equals(
				destCheckSum.generateChecksum())) {
			System.out.println("Checksum Equal, No DQ Issues Found");
			finalSummaryBean.setFinalStatus(true);
			return finalSummaryBean;
		}

		// double randomPercent = ((double) cdb.getRandomizationPrecentage()) /
		// 100;
		JavaRDD<String> srcWithoutHeaders = RDDUtils.removeHeaders(srcFile);
		JavaRDD<String> destWithoutHeaders = RDDUtils.removeHeaders(destFile);

		// Sample the data
		// JavaRDD<String> sampleSrc = srcWithoutHeaders.sample(false,
		// randomPercent);
		// JavaRDD<String> sampleDest = destWithoutHeaders.sample(false,
		// randomPercent);

		// Generate the schema based on the string of schema
		ArrayList<DataType> sourceDataType = RDDUtils
				.genrateDataTypes(srcWithoutHeaders.first());
		List<StructField> sourceFields = RDDUtils.genrateSchema(srcHeaders,
				sourceDataType);

		ArrayList<DataType> destDataType = RDDUtils
				.genrateDataTypes(destWithoutHeaders.first());
		List<StructField> destFields = RDDUtils.genrateSchema(destHeaders,
				destDataType);

		StructType sourceSchema = DataTypes.createStructType(sourceFields);
		StructType destSchema = DataTypes.createStructType(destFields);
		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> sampleSrcRowRDD = RDDUtils.genrateJavaRDD(
				srcWithoutHeaders, sourceFields);
		JavaRDD<Row> destRDD = RDDUtils.genrateJavaRDD(destWithoutHeaders,
				destFields);
		/*
		 * JavaRDD<Row> sampleDestRowRDD = RDDUtils.genrateJavaRDD(sampleDest,
		 * destFields);
		 */
		// Apply the schema to the RDD.

		DataFrame srcDataFrame = sqlContext.createDataFrame(sampleSrcRowRDD,
				sourceSchema);
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
			HashMap<String, ArrayList<String>> rules = generateRules(
					srcDataFrame, cdb.getColumnNames(), sourceFields,
					cdb.isPossibleValueRule(), cdb.isUniquenessRule());

		}
		System.out
				.println("Choose aggregation comparison operations (space saprated if more than one options).");
		System.out.println("1. SUM\n 2. AVG\n 3. MAX\n 4. MIN");
		/*
		 * BufferedReader inp = new BufferedReader (new
		 * InputStreamReader(System.in)); String[] aggOptions =
		 * inp.readLine().split("\\s+");
		 */
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
		System.out.println(sampleSrcRowRDD.count()
				+ "-----------------------------------------------------");
		finalSummaryBean = MainDriver.main(sampleSrcRowRDD, destRDD,
				srcDataFrame, types, sourceSchema, destSchema,
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
	public HashMap<String, String> getParqueteColumnsDetails(String file)
			throws FileNotFoundException {
		HashMap<String, String> colNamesTypes = new LinkedHashMap<String, String>();
		JavaSparkContext jsc = GetJavaSparkContext.getJavaSparkContex();
		org.apache.spark.sql.SQLContext sqlContext = new org.apache.spark.sql.SQLContext(
				jsc);
		StructField[] fields = sqlContext.parquetFile(file).schema().fields();
		
		for(StructField field : fields){
			colNamesTypes.put(field.name(), field.dataType().toString());
		}

		return colNamesTypes;
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
		@SuppressWarnings("resource")
		Scanner input = new Scanner(new File(file));
		String[] colNames = input.nextLine().trim()
				.split(ValueSeparater.getRegex());
		ArrayList<DataType> dataType = RDDUtils.genrateDataTypes(input
				.nextLine());
		int numRowsTried = 0;
		for (int i = 0; i < colNames.length; i++) {
			// System.out.println(colNames[i].trim());
			String type = "";
			try {
				if (dataType.get(i).equals(DataTypes.StringType)) {
					type = DataTypes.StringType.simpleString();
				} else if (dataType.get(i).equals(DataTypes.DateType)) {
					type = "Date";
				} else {
					type = "Numeric";
				}
			} catch (IndexOutOfBoundsException e) {
				if (numRowsTried < 100) {
					dataType = RDDUtils.genrateDataTypes(input.nextLine());
					i--;
					numRowsTried++;
				} else {
					type = DataTypes.StringType.simpleString();
				}
			}

			colNamesTypes.put(colNames[i].trim(), type);
		}

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
