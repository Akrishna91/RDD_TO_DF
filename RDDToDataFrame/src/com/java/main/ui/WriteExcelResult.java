package com.java.main.ui;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;




import com.java.main.beans.AggregationMismatches;
import com.java.main.beans.FinalSummaryBean;
import com.java.main.beans.RulesComparaorResult;

public class WriteExcelResult {
	
	public static void genrateReport(FinalSummaryBean finalSummary, String path) throws IOException{
		
		BufferedWriter out = null;
		try  
		{	
			path = path + "/Report.txt";
		    FileWriter fstream = new FileWriter(path, true); //true tells to append data.
		    out = new BufferedWriter(fstream);
		    HashMap<String, List<AggregationMismatches>> colNames_AggError = finalSummary.getColNames_AggError();
		    LinkedHashMap<String, RulesComparaorResult> distinctRuleResults = finalSummary.getDistinctRuleResults();
		    LinkedHashMap<String, RulesComparaorResult> possibleValueRuleResults = finalSummary.getPossibleValueRuleResults();
		    for (String colName : colNames_AggError.keySet()) {
		    	out.write(colName + ":");
		    	out.write("Errors:");
				for (AggregationMismatches error : colNames_AggError.get(colName)) {
					out.write(error.getMisMatchedFuncName() + "- \n");
					out.write("Source Value:" + error.getSourceValue() + "\n");
					out.write("Dest Value:" + error.getDestValue() + "\n");
				}
				out.write("\n");
			}
		    if (distinctRuleResults != null) {
		    	out.write("*******************Unique Columns Report**********************");
				System.out
						.println();
				for (String colName : distinctRuleResults.keySet()) {
					out.write("column name: " + colName);
					out.write(distinctRuleResults.get(colName).toString());
				}
			}

			if (possibleValueRuleResults != null) {
				System.out.println();
				out.write("\n");
				out.write("*******************Possible Value Report**********************");
				for (String colName : possibleValueRuleResults.keySet()) {
					out.write("column name: " + colName);
					out.write(possibleValueRuleResults.get(colName).toString());
				}
			}
		}
		catch (IOException e)
		{
		    System.err.println("Error: " + e.getMessage());
		}
		finally
		{
		    if(out != null) {
		        out.close();
		    }
		}
		
	}

	/*public static void main(String[] args) throws Exception {
		// Create blank workbook
		XSSFWorkbook workbook = new XSSFWorkbook();
		// Create a blank sheet
		XSSFSheet spreadsheet = workbook.createSheet(" Employee Info ");
		// Create row object
		XSSFRow row;
		// This data needs to be written (Object[])
		Map<String, Object[]> empinfo = new TreeMap<String, Object[]>();
		empinfo.put("1", new Object[] { "EMP ID", "EMP NAME", "DESIGNATION" });
		empinfo.put("2", new Object[] { "tp01", "Gopal", "Technical Manager" });
		empinfo.put("3", new Object[] { "tp02", "Manisha", "Proof Reader" });
		empinfo.put("4", new Object[] { "tp03", "Masthan", "Technical Writer" });
		empinfo.put("5", new Object[] { "tp04", "Satish", "Technical Writer" });
		empinfo.put("6", new Object[] { "tp05", "Krishna", "Technical Writer" });
		// Iterate over data and write to sheet
		Set<String> keyid = empinfo.keySet();
		int rowid = 0;
		for (String key : keyid) {
			row = spreadsheet.createRow(rowid++);
			Object[] objectArr = empinfo.get(key);
			int cellid = 0;
			for (Object obj : objectArr) {
				Cell cell = row.createCell(cellid++);
				cell.setCellValue((String) obj);
			}
		}
		// Write the workbook in file system
		FileOutputStream out = new FileOutputStream(new File(
				"C:\\Users\\kbaghel\\Desktop\\Writesheet.xlsx"));
		workbook.write(out);
		out.close();
		System.out.println("Writesheet.xlsx written successfully");
	}*/
}
