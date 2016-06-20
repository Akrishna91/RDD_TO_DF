package com.java.main.ui;

import java.util.HashMap;

import com.java.main.action.MainAction;
import com.java.main.context.GetJavaSparkContext;

/**
 * 
 * @author kbaghel Description - This class is used to do data loading
 *         background process, at the same time also shows wait bar
 */
public class DataCollectionWait {
	public void runWaitBar(final ConnectionDetailsBean ConnDtlsBean) {
		final Loading_Test obj1 = new Loading_Test("Data Loading");

		final Thread doDataCollect = new Thread(new Runnable() {
			public void run() {
				HashMap<String, String> colAndDataTypeList = new HashMap<String, String>();
				try {
					MainAction action = new MainAction();
					colAndDataTypeList = action.getColumns(ConnDtlsBean);
					
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				System.out.println("Data Collection job completed!");
				
				
				
				

				obj1.stopProgressBas();
				//Calling column and operation selection screen
				ColumnSelect obj = new ColumnSelect(ConnDtlsBean,
						colAndDataTypeList);
				obj.showEventDemo();
				
				
			}
		});

		// Starting Thread
		doDataCollect.start();

	}
}
