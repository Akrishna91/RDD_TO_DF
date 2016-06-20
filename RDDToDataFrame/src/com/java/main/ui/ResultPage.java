package com.java.main.ui;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;

import com.java.main.beans.ColResultSummaryBean;
import com.java.main.beans.FinalSummaryBean;
import com.java.main.constants.RulesMatchingStatus;

/**
 * 
 * @author kbaghel Description - This class is used to display the result
 */
public class ResultPage {

	private JFrame mainFrame;
	private JPanel basePanel, formatPanel, topPanel, resultPanel, bottomPanel;
	private JLabel qualityCheckResult;
	private JTable columnTable;
	private JScrollPane scrollPane;
	FinalSummaryBean finalSummary;
	
	ConfigurationDetailsBean configDtlsBean = new ConfigurationDetailsBean();
	ArrayList<ColResultSummaryBean> colResultSummaryBeanLst = new ArrayList<ColResultSummaryBean>();
	
	HashMap<String, ColResultSummaryBean> colSummary;
	ArrayList<String> colNamesLst = new ArrayList<String>();

	Integer noOfRows;

	/**
	 * 
	 * Description - Constructor method, which internally calls prepareGUI()
	 * method.
	 */
	public ResultPage(ConfigurationDetailsBean configDetailsBean,FinalSummaryBean finalSum) {
		configDtlsBean = configDetailsBean;
		finalSummary = finalSum;
		colSummary = finalSummary.getColResultSummaryBean();
		prepareGUI();
	}

	/**
	 * Description - This method prepares the GUI
	 */
	private void prepareGUI() {

		//Initializing main frame
		mainFrame = new JFrame("Data Quality Check");
		mainFrame.setSize(1920, 1030);
		mainFrame.setLocation(0, 0);
		mainFrame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent windowEvent) {
				System.exit(0);
			}
		});

		//Adding background image
		JLabel background = new JLabel(new ImageIcon(Path.getImagePath()));
		background.setLayout(new BoxLayout(background, BoxLayout.PAGE_AXIS));
		
		//All panel initialization
		basePanel = new JPanel();
		basePanel.setLayout(new BoxLayout(basePanel, BoxLayout.LINE_AXIS));
		basePanel.setLocation(10, 0);
		basePanel.setPreferredSize(new Dimension(100, 1000));
		basePanel.setBackground(Color.WHITE);

		formatPanel = new JPanel();
		formatPanel.setLayout(new BoxLayout(formatPanel, BoxLayout.PAGE_AXIS));
		formatPanel.setPreferredSize(new Dimension(1900, 600));
		formatPanel.setBorder(BorderFactory.createTitledBorder("Quality Check Result"));
		formatPanel.setBackground(Color.WHITE);

		topPanel = new JPanel();
		topPanel.setLayout(new FlowLayout());
		topPanel.setPreferredSize(new Dimension(1900, 150));
		topPanel.setBackground(Color.WHITE);

		bottomPanel = new JPanel();
		bottomPanel.setLayout(new FlowLayout());
		bottomPanel.setPreferredSize(new Dimension(1900, 150));
		bottomPanel.setBackground(Color.WHITE);

		resultPanel = new JPanel();
		resultPanel.setLayout(new GridLayout(0, 1));
		resultPanel.setPreferredSize(new Dimension(700, 650));
		resultPanel.setBorder(BorderFactory
				.createTitledBorder("Column Specific Result"));
		resultPanel.setBackground(Color.WHITE);

		qualityCheckResult = new JLabel("", JLabel.CENTER);
		qualityCheckResult.setFont(new Font("SansSerif", Font.BOLD, 25));
		
		//Adding all panels
		mainFrame.add(background);
		
		background.add(Box.createRigidArea(new Dimension(0, 150)));

		formatPanel.add(topPanel);
		formatPanel.add(resultPanel);
		formatPanel.add(bottomPanel);

		basePanel.add(Box.createRigidArea(new Dimension(300, 0)));
		basePanel.add(formatPanel);
		basePanel.add(Box.createRigidArea(new Dimension(300, 0)));

		background.add(basePanel);
		background.add(Box.createRigidArea(new Dimension(0, 50)));
		
		//Showing main frame
		mainFrame.setVisible(true);
	}

	/**
	 * Description - Initializes table and all other components
	 */
	public void showEventDemo() {

		// apply if else here to show result
		if(finalSummary.isFinalStatus()){
			qualityCheckResult.setText("No Mismatch Found");
		}
		else{
			qualityCheckResult.setText("Mismatch Found");
		}
		

		//Adding Sample data, should be removed after integration - Start
		/*ColResultSummaryBean cSummaryBean = new ColResultSummaryBean();
		cSummaryBean.setColumnName("Column1");
		cSummaryBean.setPossibleValueRuleResult(RulesMatchingStatus.MIGHT_MATCH);
		cSummaryBean.setSummationRuleResult(RulesMatchingStatus.MIGHT_MATCH);
		cSummaryBean.setMinimumRuleResult(RulesMatchingStatus.MATCHED);
		cSummaryBean.setMaximumRuleResult(RulesMatchingStatus.MISMATCHED);
		cSummaryBean.setMeanRuleResult(RulesMatchingStatus.MATCHED);
		colResultSummaryBeanLst.add(cSummaryBean);
		
		ColResultSummaryBean cBean = new ColResultSummaryBean();
		cBean.setColumnName("Column2");
		cBean.setPossibleValueRuleResult(RulesMatchingStatus.MATCHED);
		cBean.setSummationRuleResult(RulesMatchingStatus.MIGHT_MATCH);
		cBean.setMinimumRuleResult(RulesMatchingStatus.MATCHED);
		cBean.setMaximumRuleResult(RulesMatchingStatus.MISMATCHED);
		cBean.setMeanRuleResult(RulesMatchingStatus.MATCHED);
		colResultSummaryBeanLst.add(cBean);
		
		cBean = new ColResultSummaryBean();
		cBean.setColumnName("Column3");
		cBean.setPossibleValueRuleResult(RulesMatchingStatus.MATCHED);
		cBean.setSummationRuleResult(RulesMatchingStatus.MIGHT_MATCH);
		cBean.setMinimumRuleResult(RulesMatchingStatus.MATCHED);
		cBean.setMaximumRuleResult(RulesMatchingStatus.MISMATCHED);
		cBean.setMeanRuleResult(RulesMatchingStatus.MATCHED);
		colResultSummaryBeanLst.add(cBean);*/
		//Adding Sample data, should be removed after integration - End
		
		// Create data
		//colNamesLst.addAll(colSummary.keySet());
		colNamesLst.addAll(configDtlsBean.getColumnNames());
		
		noOfRows = colNamesLst.size();

		// Create columns names
		String columnNames[] = { "Column Name", "Uniqueness",
				"Possible Values", "Date Type", "Summation", "Minimum",
				"Maximum", "Mean", "Mode" };
		String dataValues[][] = new String[colNamesLst.size()][9];

		// Adding column name data in table
		for (int i = 0; i < colNamesLst.size(); i++) {
			dataValues[i][0] = colNamesLst.get(i);
		}

		// Create a new table instance
		DefaultTableModel model = new DefaultTableModel(dataValues, columnNames);
		columnTable = new JTable(model) {

			private static final long serialVersionUID = 1L;

			/*
			 * @Override public Class getColumnClass(int column) { return
			 * getValueAt(0, column).getClass(); }
			 */
			@Override
			/**
			 * Description - Setting data types of columns in table
			 */
			public Class getColumnClass(int column) {
				switch (column) {
				case 0:
				    return String.class;
				case 1:
				    return String.class;
				default:
					return String.class;
				}
			}
			/**
			 * Description - Making all cells non editable
			 */
			public boolean isCellEditable(int rowIndex, int colIndex) {
				return false;
			}

			/**
			 * Description - Setting color of each cell in table
			 */
			@Override
			public Component prepareRenderer(TableCellRenderer renderer,
					int row, int col) {
				Component comp = super.prepareRenderer(renderer, row, col);
				String value = (String) getModel().getValueAt(row, 0);

				if (col == 1 && !configDtlsBean.isUniquenessRule()) {
					comp.setBackground(Color.lightGray);
				} else if (col == 2 && !configDtlsBean.isPossibleValueRule()) {
					comp.setBackground(Color.lightGray);
				} else if (col == 3 && !configDtlsBean.isDateTypeRule()) {
					comp.setBackground(Color.lightGray);
				} else if (col == 4 && !configDtlsBean.isSummationRule()) {
					comp.setBackground(Color.lightGray);
				} else if (col == 5 && !configDtlsBean.isMinimumRule()) {
					comp.setBackground(Color.lightGray);
				} else if (col == 6 && !configDtlsBean.isMaximumRule()) {
					comp.setBackground(Color.lightGray);
				} else if (col == 7 && !configDtlsBean.isMeanRule()) {
					comp.setBackground(Color.lightGray);
				} else if (col == 8 && !configDtlsBean.isModeRule()) {
					comp.setBackground(Color.lightGray);
				} else {
					if(finalSummary.isFinalStatus()){
						if(col == 0){
							comp.setBackground(Color.white);
						}
						else{
							comp.setBackground(Color.green);
						}
						return comp;
					}
					else{
						ColResultSummaryBean colSummaryBean = colSummary.get(value);
						if (col == 1
								&& colSummaryBean
										.getUniquenessRuleResult() == null) {
							comp.setBackground(Color.lightGray);
							return comp;
						}
						else if (col == 1
								&& colSummaryBean
										.getUniquenessRuleResult().equals(RulesMatchingStatus.MATCHED)) {
							comp.setBackground(Color.green);
							return comp;
						} 
						else if (col == 1
								&& colSummaryBean
										.getUniquenessRuleResult().equals(RulesMatchingStatus.MIGHT_MATCH)) {
							comp.setBackground(Color.orange);
							return comp;
						} 
						else if (col == 1
								&& colSummaryBean
										.getUniquenessRuleResult().equals(RulesMatchingStatus.MISMATCHED)) {
							comp.setBackground(Color.red);
							return comp;
						} 
						else if (col == 2
								&& colSummaryBean
										.getPossibleValueRuleResult() == null) {
							comp.setBackground(Color.lightGray);
							return comp;
						} 
						else if (col == 2
								&& colSummaryBean
										.getPossibleValueRuleResult().equals(RulesMatchingStatus.MATCHED)) {
							comp.setBackground(Color.green);
							return comp;
						} 
						else if (col == 2
								&& colSummaryBean
										.getPossibleValueRuleResult().equals(RulesMatchingStatus.MIGHT_MATCH)) {
							comp.setBackground(Color.orange);
							return comp;
						} 
						else if (col == 2
								&& colSummaryBean
										.getPossibleValueRuleResult().equals(RulesMatchingStatus.MISMATCHED)) {
							comp.setBackground(Color.red);
							return comp;
						} 
						else if (col == 3
								&& colSummaryBean
										.getDateTypeRuleResult() == null) {
							comp.setBackground(Color.lightGray);
							return comp;
						} 
						else if (col == 3
								&& colSummaryBean
										.getDateTypeRuleResult().equals(RulesMatchingStatus.MATCHED)) {
							comp.setBackground(Color.green);
							return comp;
						} 
						else if (col == 3
								&& colSummaryBean
										.getDateTypeRuleResult().equals(RulesMatchingStatus.MIGHT_MATCH)) {
							comp.setBackground(Color.orange);
							return comp;
						} 
						else if (col == 3
								&& colSummaryBean
										.getDateTypeRuleResult().equals(RulesMatchingStatus.MISMATCHED)) {
							comp.setBackground(Color.red);
							return comp;
						} 
						else if (col == 4
								&& colSummaryBean
										.getSummationRuleResult().equals(RulesMatchingStatus.MATCHED)) {
							comp.setBackground(Color.green);
							return comp;
						} 
						else if (col == 4
								&& colSummaryBean
										.getSummationRuleResult().equals(RulesMatchingStatus.MIGHT_MATCH)) {
							comp.setBackground(Color.orange);
							return comp;
						} 
						else if (col == 4
								&& colSummaryBean
										.getSummationRuleResult().equals(RulesMatchingStatus.MISMATCHED)) {
							comp.setBackground(Color.red);
							return comp;
						} 
						else if (col == 5
								&& colSummaryBean.getMinimumRuleResult().equals(RulesMatchingStatus.MATCHED)) {
							comp.setBackground(Color.green);
							return comp;
						} 
						else if (col == 5
								&& colSummaryBean
										.getMinimumRuleResult().equals(RulesMatchingStatus.MIGHT_MATCH)) {
							comp.setBackground(Color.orange);
							return comp;
						} 
						else if (col == 5
								&& colSummaryBean
										.getMinimumRuleResult().equals(RulesMatchingStatus.MISMATCHED)) {
							comp.setBackground(Color.red);
							return comp;
						} 
						else if (col == 6
								&& colSummaryBean
										.getMaximumRuleResult().equals(RulesMatchingStatus.MATCHED)) {
							comp.setBackground(Color.green);
							return comp;
						} 
						else if (col == 6
								&& colSummaryBean
										.getMaximumRuleResult().equals(RulesMatchingStatus.MIGHT_MATCH)) {
							comp.setBackground(Color.orange);
							return comp;
						} 
						else if (col == 6
								&& colSummaryBean
										.getMaximumRuleResult().equals(RulesMatchingStatus.MISMATCHED)) {
							comp.setBackground(Color.red);
							return comp;
						} 
						else if (col == 7
								&& colSummaryBean
										.getMeanRuleResult().equals(RulesMatchingStatus.MATCHED)) {
							comp.setBackground(Color.green);
							return comp;
						} 
						else if (col == 7
								&& colSummaryBean
										.getMeanRuleResult().equals(RulesMatchingStatus.MIGHT_MATCH)) {
							comp.setBackground(Color.orange);
							return comp;
						} 
						else if (col == 7
								&& colSummaryBean
										.getMeanRuleResult().equals(RulesMatchingStatus.MISMATCHED)) {
							comp.setBackground(Color.red);
							return comp;
						} 
						else if (col == 8
								&& colSummaryBean
										.getModeRuleResult().equals(RulesMatchingStatus.MATCHED)) {
							comp.setBackground(Color.green);
							return comp;
						} 
						else if (col == 8
								&& colSummaryBean
										.getModeRuleResult().equals(RulesMatchingStatus.MIGHT_MATCH)) {
							comp.setBackground(Color.orange);
							return comp;
						} 
						else if (col == 8
								&& colSummaryBean
										.getModeRuleResult().equals(RulesMatchingStatus.MISMATCHED)) {
							comp.setBackground(Color.red);
							return comp;
						} else {
							comp.setBackground(Color.white);
							return comp;
						}
					}
				}

				return comp;
			}
		};

		columnTable.setPreferredScrollableViewportSize(columnTable
				.getPreferredSize());
		columnTable.getColumn("Column Name").setMinWidth(250);
		columnTable.getColumn("Column Name").setMaxWidth(250);
		columnTable.getColumn("Uniqueness").setMinWidth(125);
		columnTable.getColumn("Uniqueness").setMaxWidth(125);
		columnTable.getColumn("Possible Values").setMinWidth(125);
		columnTable.getColumn("Possible Values").setMaxWidth(125);
		columnTable.getColumn("Date Type").setMinWidth(125);
		columnTable.getColumn("Date Type").setMaxWidth(125);
		columnTable.getColumn("Summation").setMinWidth(125);
		columnTable.getColumn("Summation").setMaxWidth(125);
		columnTable.getColumn("Minimum").setMinWidth(125);
		columnTable.getColumn("Minimum").setMaxWidth(125);
		columnTable.getColumn("Maximum").setMinWidth(125);
		columnTable.getColumn("Maximum").setMaxWidth(125);
		columnTable.getColumn("Mean").setMinWidth(125);
		columnTable.getColumn("Mean").setMaxWidth(125);
		columnTable.getColumn("Mode").setMinWidth(125);
		columnTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);

		columnTable.setMinimumSize(new Dimension(600, 0));

		// Add the table to a scrolling pane
		scrollPane = new JScrollPane(columnTable);
		JScrollBar srl = new JScrollBar();
		scrollPane.setVerticalScrollBar(srl);
		scrollPane.setMinimumSize(new Dimension(600, 0));

		columnTable.setEnabled(true);

		JButton downloadButton = new JButton("Download Detailed Report");
		JButton homeButton = new JButton("Home");
		JButton closeButton = new JButton("Close");

		downloadButton.setActionCommand("download");
		homeButton.setActionCommand("home");
		closeButton.setActionCommand("close");

		downloadButton.addActionListener(new ButtonClickListener());
		homeButton.addActionListener(new ButtonClickListener());
		closeButton.addActionListener(new ButtonClickListener());

		topPanel.add(qualityCheckResult);
		topPanel.add(Box.createRigidArea(new Dimension(100, 0)));
		topPanel.add(downloadButton);

		resultPanel.add(scrollPane);

		bottomPanel.add(homeButton);
		bottomPanel.add(closeButton);
		
		//Disabling download button if no mismatch found
		if(finalSummary.isFinalStatus()){
			downloadButton.setEnabled(false);
		}

		mainFrame.setVisible(true);

		//Blinking the final result
		for (int i = 0; i < 20; i++) {
			if (i % 2 == 0) {
				qualityCheckResult.setForeground(Color.red);
			} else {
				qualityCheckResult.setForeground(Color.BLUE);
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	private class ButtonClickListener implements ActionListener {
		public void actionPerformed(ActionEvent e) {
			String command = e.getActionCommand();
			if (command.equals("download")) {
				JFileChooser chooser = new JFileChooser(); 
                chooser.setCurrentDirectory(new java.io.File("."));
                chooser.setDialogTitle("Select location to save file");
                chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
                chooser.setAcceptAllFileFilterUsed(false);
                    
                if (chooser.showOpenDialog(chooser) == JFileChooser.APPROVE_OPTION) { 
                  String folderPath = chooser.getSelectedFile().getAbsolutePath();
                  try {
                	  ReportGenrator.genrateReport(configDtlsBean, finalSummary, folderPath);
					//WriteExcelResult.genrateReport(finalSummary, folderPath);
                	  JOptionPane.showMessageDialog(null,
								"File downloaded successfully.");
				} catch (Exception e1) {
					e1.printStackTrace();
				}                  
                  }
                else {
                  System.out.println("No Selection ");
                  }


			} else if (command.equals("home")) {
				int opcion = JOptionPane.showConfirmDialog(null,
						"Do you want to go to home page ?", "Confirmation",
						JOptionPane.YES_NO_OPTION);

				if (opcion == 0) {
					mainFrame.setVisible(false);
					HomePage swingControlDemo = new HomePage();
					swingControlDemo.showEventDemo();
				} else {
				}
			} else if (command.equals("close")) {
				int opcion = JOptionPane.showConfirmDialog(null,
						"Do you want to close application ?", "Confirmation",
						JOptionPane.YES_NO_OPTION);

				if (opcion == 0) {
					System.exit(0);
				} else {
				}

			}
		}
	}
}
