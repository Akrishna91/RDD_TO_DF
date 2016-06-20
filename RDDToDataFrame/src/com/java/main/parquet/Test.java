package com.java.main.parquet;

import java.io.FileNotFoundException;
import java.util.HashMap;

public class Test {
	
	public static void main(String[] args) throws FileNotFoundException{
		Processor p = new Processor();
		//HashMap<String, String> colTypes = p.getColumnsDetails("/home/cloudera/Desktop/td_store_dim");
		
		p.compare("/home/cloudera/Desktop/td_store_dim", "/home/cloudera/Desktop/hana_store_dim");
		//p.getColumnsDetails("/home/cloudera/Desktop/hana_store_dim");
		/*for(String colType : colTypes.keySet()){
			System.out.println(colType  +": " + colTypes.get(colType));
		}*/
	}

}
