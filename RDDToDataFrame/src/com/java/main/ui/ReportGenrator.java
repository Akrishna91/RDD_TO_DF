package com.java.main.ui;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.poi.hssf.util.CellRangeAddress;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.Row;

import com.java.main.beans.AggregationMismatches;
import com.java.main.beans.FinalSummaryBean;
import com.java.main.constants.AggregationFuncNames;
import com.java.main.constants.RulesMatchingStatus;

public class ReportGenrator {
	public static void genrateReport(ConfigurationDetailsBean ConfigBean,
			FinalSummaryBean finalSummary, String path) throws Exception {

		// Create blank workbook
		XSSFWorkbook workbook = new XSSFWorkbook();
		// Create a blank sheet
		XSSFSheet spreadsheet = workbook.createSheet("Column Report");
		// Create row object
		XSSFRow row;

		// Creating Header
		int colNo = 0;
		Cell headerCell;

		XSSFRow colRow;
		Cell colCell;
		int cellNo = 1;
		for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
			colRow = spreadsheet.createRow(cellNo);
			colCell = colRow.createCell(0);
			colCell.setCellValue(ConfigBean.getColumnNames().get(i));
			spreadsheet.addMergedRegion(new CellRangeAddress(cellNo,
					cellNo + 1, 0, 0));
			colRow = spreadsheet.createRow(cellNo + 1);
			cellNo = cellNo + 2;
		}

		row = spreadsheet.createRow(0);

		headerCell = row.createCell(colNo++);
		headerCell.setCellValue("Column Name");

		if (ConfigBean.isUniquenessRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Uniqueness");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);

				resultRow = spreadsheet.getRow(rowNum);

				if ((finalSummary.getDistinctRuleResults().get(colName) != null)
						&& !finalSummary.getDistinctRuleResults().get(colName)
								.getStatus()
								.equals(RulesMatchingStatus.MATCHED)) {
					resultCell = resultRow.createCell(colNum);
					resultCell.setCellValue("Src - "
							+ finalSummary.getDistinctRuleResults()
									.get(colName).getSourceValues());

					resultCell = resultRow.createCell(colNum + 1);
					resultCell.setCellValue("Dest - "
							+ finalSummary.getDistinctRuleResults()
									.get(colName).getDestValues());
				} else {
					spreadsheet.addMergedRegion(new CellRangeAddress(rowNum,
							rowNum, colNum, colNum + 1));
				}

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				try {
					resultCell.setCellValue("Result - "
							+ finalSummary.getDistinctRuleResults()
									.get(colName).getStatus());
				} catch (NullPointerException e) {
					resultCell.setCellValue("Result - " + "No Rule Genrated");
				}

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}

		if (ConfigBean.isPossibleValueRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Possible Values");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);
				resultRow = spreadsheet.getRow(rowNum);

				if ((finalSummary.getPossibleValueRuleResults().get(colName) != null)
						&& !finalSummary.getPossibleValueRuleResults()
								.get(colName).getStatus()
								.equals(RulesMatchingStatus.MATCHED)) {
					resultCell = resultRow.createCell(colNum);
					resultCell.setCellValue("Src - "
							+ finalSummary.getPossibleValueRuleResults()
									.get(colName).getSourceValues());

					resultCell = resultRow.createCell(colNum + 1);
					resultCell.setCellValue("Dest - "
							+ finalSummary.getPossibleValueRuleResults()
									.get(colName).getDestValues());
				}

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				try {
					resultCell.setCellValue("Result - "
							+ finalSummary.getPossibleValueRuleResults()
									.get(colName).getStatus());

				} catch (NullPointerException e) {
					resultCell.setCellValue("Result - " + "No Rule Genrated");
				}

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}

		if (ConfigBean.isDateTypeRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Date Type");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);
				resultRow = spreadsheet.getRow(rowNum);
				resultCell = resultRow.createCell(colNum);
				resultCell.setCellValue("Src - " + "100");

				resultCell = resultRow.createCell(colNum + 1);
				resultCell.setCellValue("Dest - " + "100");

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				resultCell.setCellValue("Result - " + "Matched");

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}
		HashMap<String, List<AggregationMismatches>> colNames_AggError = finalSummary
				.getColNames_AggError();
		HashMap<String, HashMap<String, AggregationMismatches>> colName_AggError_value = new HashMap<String, HashMap<String, AggregationMismatches>>();
		for (String colName : colNames_AggError.keySet()) {
			colName_AggError_value.put(colName,
					new HashMap<String, AggregationMismatches>());
			List<AggregationMismatches> temps = colNames_AggError.get(colName);
			HashMap<String, AggregationMismatches> currentAgg = new HashMap<String, AggregationMismatches>();
			for (AggregationMismatches temp1 : temps) {

				currentAgg.put(temp1.getMisMatchedFuncName().toString(), temp1);
			}
			colName_AggError_value.put(colName, currentAgg);
		}
		if (ConfigBean.isSummationRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Summation");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);
				resultRow = spreadsheet.getRow(rowNum);

				if (!finalSummary.getColResultSummaryBean().get(colName)
						.getSummationRuleResult()
						.equals(RulesMatchingStatus.MATCHED)) {
					resultCell = resultRow.createCell(colNum);
					resultCell.setCellValue("Src - "
							+ colName_AggError_value.get(colName)
									.get(AggregationFuncNames.SUM.toString())
									.getSourceValue());

					resultCell = resultRow.createCell(colNum + 1);
					resultCell.setCellValue("Dest - "
							+ colName_AggError_value.get(colName)
									.get(AggregationFuncNames.SUM.toString())
									.getDestValue());
				} else {
					spreadsheet.addMergedRegion(new CellRangeAddress(rowNum,
							rowNum, colNum, colNum + 1));
				}

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				resultCell.setCellValue("Result - "
						+ finalSummary.getColResultSummaryBean().get(colName)
								.getSummationRuleResult());

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}

		if (ConfigBean.isMinimumRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Minimum");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);
				resultRow = spreadsheet.getRow(rowNum);

				if (!finalSummary.getColResultSummaryBean().get(colName)
						.getMinimumRuleResult()
						.equals(RulesMatchingStatus.MATCHED)) {
					resultCell = resultRow.createCell(colNum);
					resultCell.setCellValue("Src - "
							+ colName_AggError_value.get(colName)
									.get(AggregationFuncNames.MIN.toString())
									.getSourceValue());

					resultCell = resultRow.createCell(colNum + 1);
					resultCell.setCellValue("Dest - "
							+ colName_AggError_value.get(colName)
									.get(AggregationFuncNames.MIN.toString())
									.getDestValue());
				} else {
					spreadsheet.addMergedRegion(new CellRangeAddress(rowNum,
							rowNum, colNum, colNum + 1));
				}

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				resultCell.setCellValue("Result - "
						+ finalSummary.getColResultSummaryBean().get(colName)
								.getMinimumRuleResult());

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}

		if (ConfigBean.isMaximumRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Maximum");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);
				resultRow = spreadsheet.getRow(rowNum);

				if (!finalSummary.getColResultSummaryBean().get(colName)
						.getMaximumRuleResult()
						.equals(RulesMatchingStatus.MATCHED)) {
					resultCell = resultRow.createCell(colNum);
					resultCell.setCellValue("Src - "
							+ colName_AggError_value.get(colName)
									.get(AggregationFuncNames.MAX.toString())
									.getSourceValue());

					resultCell = resultRow.createCell(colNum + 1);
					resultCell.setCellValue("Dest - "
							+ colName_AggError_value.get(colName)
									.get(AggregationFuncNames.MAX.toString())
									.getDestValue());
				} else {
					spreadsheet.addMergedRegion(new CellRangeAddress(rowNum,
							rowNum, colNum, colNum + 1));
				}

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				resultCell.setCellValue("Result - "
						+ finalSummary.getColResultSummaryBean().get(colName)
								.getMaximumRuleResult());

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}

		if (ConfigBean.isMeanRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Mean");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);
				resultRow = spreadsheet.getRow(rowNum);

				if (!finalSummary.getColResultSummaryBean().get(colName)
						.getMeanRuleResult()
						.equals(RulesMatchingStatus.MATCHED)) {
					resultCell = resultRow.createCell(colNum);
					resultCell.setCellValue("Src - "
							+ colName_AggError_value.get(colName)
									.get(AggregationFuncNames.AVG.toString())
									.getSourceValue());

					resultCell = resultRow.createCell(colNum + 1);
					resultCell.setCellValue("Dest - "
							+ colName_AggError_value.get(colName)
									.get(AggregationFuncNames.AVG.toString())
									.getDestValue());
				} else {
					spreadsheet.addMergedRegion(new CellRangeAddress(rowNum,
							rowNum, colNum, colNum + 1));
				}

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				resultCell.setCellValue("Result - "
						+ finalSummary.getColResultSummaryBean().get(colName)
								.getMeanRuleResult());

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}

		if (ConfigBean.isModeRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Mode");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);
				resultRow = spreadsheet.getRow(rowNum);
				resultCell = resultRow.createCell(colNum);
				resultCell.setCellValue("Src - " + "100");

				resultCell = resultRow.createCell(colNum + 1);
				resultCell.setCellValue("Dest - " + "100");

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				resultCell.setCellValue("Result - " + "Matched");

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}

		// Creating mismatched row sheet - Start
		XSSFSheet mismatchedRowSheet = workbook.createSheet("Mismatched Rows");
		// Create row object
		XSSFRow rows;
		// This data needs to be written (Object[])
		LinkedHashMap<String, String> rowinfo = new LinkedHashMap<String, String>();

		rowinfo.put("Sr. No.", "Mismatched Row");
		int i = 0;
		for (Row row1 : finalSummary.getMissingRows()) {
			rowinfo.put(i + "", row1.toString());
			i++;
		}

		// Iterate over data and write to sheet
		Set<String> newKeyid = rowinfo.keySet();
		int newRowId = 0;

		for (String key : newKeyid) {
			rows = mismatchedRowSheet.createRow(newRowId++);
			Cell cell1 = rows.createCell(0);
			cell1.setCellValue(key);

			Cell cell2 = rows.createCell(1);
			cell2.setCellValue(rowinfo.get(key));
		}

		// Creating mismatched row sheet - End

		// Write the workbook in file system
		FileOutputStream out = new FileOutputStream(new File(path
				+ "/QualityCheckReport.xlsx"));
		workbook.write(out);
		out.close();
		System.out.println("QualityCheckReport.xlsx written successfully");
	}
}
