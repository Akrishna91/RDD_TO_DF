package com.java.main.utils;

import java.util.ArrayList;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class MatchedRecords {

	//private FileDataFrames fileDFs;
	private ArrayList<String> pkey;
	private SQLContext sqlContext;
	private DataFrame srcDf;
	private DataFrame destDf;/*

	public MatchedRecords(FileDataFrames fileDFs, ArrayList<String> pkey, SQLContext sqlContext) {
		this.fileDFs = fileDFs;
		setSrcDf(fileDFs.getSourceDataFrame());
		setDestDf(fileDFs.getDestDataFrame());
		this.pkey = pkey;
		this.sqlContext = sqlContext;
	}*/
	
	public MatchedRecords(DataFrame srcDf, DataFrame destDf, ArrayList<String> pkey, SQLContext sqlContext) {
		this.srcDf = srcDf;
		this.destDf = destDf;
		this.pkey = pkey;
		this.sqlContext = sqlContext;
	}
	
	public DataFrame fetMatchingRecords() {
		DataFrame srcDf = getSrcDf();
		DataFrame destDf = getDestDf();
		srcDf.registerTempTable("SrcTable");
		destDf.registerTempTable("DestTable");
		DataFrame records = sqlContext.sql(getSQL());
		records.show();
		return records;
	}
	
	private String getSQL() {
		String sql = "SELECT d.* FROM DestTable d INNER JOIN SrcTable s "+getWhereCondition();
		//String sql = "SELECT d.* FROM DestTable d where d._id in (1,2,3)";
		System.out.println("SQL-->"+sql);
		return sql;
	}

	private String getWhereCondition() {
		String whereCondition = "";
		int length = 1;
		if(pkey.size()>0){
			whereCondition+="ON ";
			for(String key : pkey){
				//whereCondition+=("s."+key+" IS NOT NULL AND d."+key+ " IS NOT NULL AND ");
				whereCondition+=("s."+key+" = d."+key);
				if(!(length==pkey.size())){
					whereCondition+=" AND ";
					length++;
				}
			}
		}
		return whereCondition;
	}


	public ArrayList<String> getPkey() {
		return pkey;
	}

	public void setPkey(ArrayList<String> pkey) {
		this.pkey = pkey;
	}

	public DataFrame getSrcDf() {
		return srcDf;
	}

	public void setSrcDf(DataFrame srcDf) {
		this.srcDf = srcDf;
	}

	public DataFrame getDestDf() {
		return destDf;
	}

	public void setDestDf(DataFrame destDf) {
		this.destDf = destDf;
	}	

}
