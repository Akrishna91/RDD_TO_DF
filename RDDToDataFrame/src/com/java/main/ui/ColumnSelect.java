package com.java.main.ui;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.DefaultTableModel;

/**
 * 
 * @author kbaghel Description - This class is used to generate column and data
 *         quality operation selection screen
 */
public class ColumnSelect {

	private JFrame mainFrame;
	private JPanel basePanel, formatPanel, topPanel, dataPanel, columnPanel,
			taskPanel, bottomPanel;
	private JCheckBox uniqueNess, possibleValues, dateType, sum, min, max,
			mean, mode;
	private JLabel randomization, keyTypeLabel;
	private JTextField textRandomization;
	private JComboBox keyTypeCombo;
	private JTable columnTable;
	private JScrollPane scrollPane;

	Integer colLength;

	ConnectionDetailsBean ConnDetailsBean = new ConnectionDetailsBean();
	ArrayList<String> colNamesLst = new ArrayList<String>();
	ArrayList<String> colDataTypeLst = new ArrayList<String>();

	public ColumnSelect(ConnectionDetailsBean connBean,
			HashMap<String, String> colDataTypeList) {
		ConnDetailsBean = connBean;

		Iterator entries = colDataTypeList.entrySet().iterator();
		while (entries.hasNext()) {
			Map.Entry entry = (Map.Entry) entries.next();
			colNamesLst.add((String) entry.getKey());
			colDataTypeLst.add((String) entry.getValue());
		}
		colLength = colNamesLst.size();

		prepareGUI();
	}

	private void prepareGUI() {
		mainFrame = new JFrame("Data Quality Check");
		mainFrame.setSize(1920, 1030);
		mainFrame.setLocation(0, 0);
		mainFrame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent windowEvent) {
				System.exit(0);
			}
		});
	

		JLabel background = new JLabel(new ImageIcon(Path.getImagePath()));
		
		background.setLayout(new BoxLayout(background, BoxLayout.PAGE_AXIS));

		//Initializing Labels
		randomization = new JLabel("", JLabel.CENTER);
		keyTypeLabel = new JLabel("", JLabel.CENTER);

		//Initializing check boxes
		uniqueNess = new JCheckBox("Uniqueness");
		uniqueNess.setSelected(true);

		possibleValues = new JCheckBox("Possible Values");
		possibleValues.setSelected(true);

		dateType = new JCheckBox("Date Type");
		dateType.setSelected(false);
		//dateType.setEnabled(false);

		sum = new JCheckBox("Summation");
		sum.setSelected(true);

		min = new JCheckBox("Minimum");
		min.setSelected(true);

		max = new JCheckBox("Maximum");
		max.setSelected(true);

		mean = new JCheckBox("Mean");
		mean.setSelected(true);

		mode = new JCheckBox("Mode");
		mode.setSelected(false);
		mode.setEnabled(false); // Disabled temporarily because this operation is not yet implemented

		//Initializing panels
		basePanel = new JPanel();
		basePanel.setLayout(new BoxLayout(basePanel, BoxLayout.LINE_AXIS));
		basePanel.setLocation(10, 0);
		basePanel.setPreferredSize(new Dimension(100, 1000));
		basePanel.setBackground(Color.WHITE);

		formatPanel = new JPanel();
		formatPanel.setLayout(new BoxLayout(formatPanel, BoxLayout.PAGE_AXIS));
		formatPanel.setPreferredSize(new Dimension(1900, 600));
		formatPanel
				.setBorder(BorderFactory.createTitledBorder("Configuration"));
		formatPanel.setBackground(Color.WHITE);

		topPanel = new JPanel();
		topPanel.setLayout(new FlowLayout());
		topPanel.setPreferredSize(new Dimension(1900, 150));
		topPanel.setBackground(Color.WHITE);

		bottomPanel = new JPanel();
		bottomPanel.setLayout(new FlowLayout());
		bottomPanel.setPreferredSize(new Dimension(1900, 150));
		bottomPanel.setBackground(Color.WHITE);

		dataPanel = new JPanel();
		dataPanel.setLayout(new BoxLayout(dataPanel, BoxLayout.LINE_AXIS));
		dataPanel.setPreferredSize(new Dimension(700, 650));
		dataPanel.setBorder(BorderFactory
				.createTitledBorder("Column and Operations"));
		dataPanel.setBackground(Color.WHITE);

		columnPanel = new JPanel();
		columnPanel.setLayout(new GridLayout(0, 1));
		columnPanel.setPreferredSize(new Dimension(1200, 150));
		columnPanel.setBorder(BorderFactory
				.createTitledBorder("Select Columns"));
		columnPanel.setBackground(Color.WHITE);

		taskPanel = new JPanel();
		taskPanel.setLayout(new GridLayout(0, 2));
		taskPanel.setPreferredSize(new Dimension(1200, 200));
		taskPanel.setBorder(BorderFactory
				.createTitledBorder("Select Operations"));
		taskPanel.setBackground(Color.WHITE);

		// Adding all components
		mainFrame.add(background);
		background.add(Box.createRigidArea(new Dimension(0, 150)));

		dataPanel.add(Box.createRigidArea(new Dimension(80, 0)));
		dataPanel.add(columnPanel);
		dataPanel.add(Box.createRigidArea(new Dimension(50, 0)));
		dataPanel.add(taskPanel);
		dataPanel.add(Box.createRigidArea(new Dimension(80, 0)));

		formatPanel.add(topPanel);
		formatPanel.add(dataPanel);
		formatPanel.add(bottomPanel);

		basePanel.add(Box.createRigidArea(new Dimension(300, 0)));
		basePanel.add(formatPanel);
		basePanel.add(Box.createRigidArea(new Dimension(300, 0)));

		background.add(basePanel);
		background.add(Box.createRigidArea(new Dimension(0, 50)));
		mainFrame.setVisible(true);
	}

	public void showEventDemo() {

		randomization.setText("Randomization Percentage");
		keyTypeLabel.setText("Key Type");

		textRandomization = new JTextField(20);

		String[] keyTypeList = { "Composit Key", "Primary Key", "No Key" };

		keyTypeCombo = new JComboBox(keyTypeList);
		keyTypeCombo.setSelectedIndex(0);
		
		// Create columns names
		String columnNames[] = { "Column Name", "Data Type",
				"Select for Quality Check", "Key" };
		String dataValues[][] = new String[colNamesLst.size()][4];

		// Create some data
		for (int i = 0; i < colNamesLst.size(); i++) {
			dataValues[i][0] = colNamesLst.get(i);
			dataValues[i][1] = colDataTypeLst.get(i);
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
			public Class getColumnClass(int column) {
				switch (column) {
				case 0:
					return String.class;
				case 1:
					return String.class;
				default:
					return Boolean.class;
				}
			}

			public boolean isCellEditable(int rowIndex, int colIndex) {
				String keyTypeValue = (String) keyTypeCombo.getSelectedItem();
				if (colIndex == 3 && keyTypeValue.equalsIgnoreCase("No Key")) {
					return false; // Disallow the editing of any cell
				} else if (colIndex == 3
						&& keyTypeValue.equalsIgnoreCase("Primary Key")) {
					int noOfSelectedCells = 0;
					int x = 0;
					int y = 0;
					for (int i = 0; i < colLength; i++) {
						if (columnTable.getModel().getValueAt(i, 3) != null
								&& columnTable.getModel().getValueAt(i, 3)
										.equals(true)) {
							noOfSelectedCells = noOfSelectedCells + 1;
							x = i;
							y = 3;
						}
					}
					if (noOfSelectedCells == 1 && x == rowIndex
							&& y == colIndex) {
						return true;
					} else if (noOfSelectedCells == 0) {
						return true;
					} else {
						return false;
					}
				} else {
					return true;
				}
			}
		};

		columnTable.setPreferredScrollableViewportSize(columnTable
				.getPreferredSize());
		columnTable.getColumn("Column Name").setMinWidth(150);
		columnTable.getColumn("Column Name").setMaxWidth(150);
		columnTable.getColumn("Data Type").setMinWidth(150);
		columnTable.getColumn("Data Type").setMaxWidth(150);
		columnTable.getColumn("Select for Quality Check").setMinWidth(150);
		columnTable.getColumn("Select for Quality Check").setMaxWidth(150);
		columnTable.getColumn("Key").setMinWidth(150);
		columnTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);

		columnTable.setMinimumSize(new Dimension(600, 0));

		// Add the table to a scrolling pane
		scrollPane = new JScrollPane(columnTable);
		JScrollBar srl = new JScrollBar();
		scrollPane.setVerticalScrollBar(srl);
		scrollPane.setMinimumSize(new Dimension(600, 0));

		columnTable.setEnabled(true);

		JButton submitButton = new JButton("Submit");
		JButton resetButton = new JButton("Reset");
		JButton backButton = new JButton("Back");

		keyTypeCombo.setActionCommand("keyType");
		resetButton.setActionCommand("reset");
		submitButton.setActionCommand("submit");
		backButton.setActionCommand("back");

		keyTypeCombo.addActionListener(new ButtonClickListener());
		resetButton.addActionListener(new ButtonClickListener());
		submitButton.addActionListener(new ButtonClickListener());
		backButton.addActionListener(new ButtonClickListener());

		topPanel.add(randomization);
		topPanel.add(textRandomization);
		topPanel.add(Box.createRigidArea(new Dimension(100, 0)));
		topPanel.add(keyTypeLabel);
		topPanel.add(keyTypeCombo);

		columnPanel.add(scrollPane);

		taskPanel.add(uniqueNess);
		taskPanel.add(possibleValues);
		taskPanel.add(dateType);
		taskPanel.add(sum);
		taskPanel.add(min);
		taskPanel.add(max);
		taskPanel.add(mean);
		taskPanel.add(mode);

		bottomPanel.add(submitButton);
		bottomPanel.add(resetButton);
		bottomPanel.add(backButton);

		mainFrame.setVisible(true);
	}

	private class ButtonClickListener implements ActionListener {
		public void actionPerformed(ActionEvent e) {
			String command = e.getActionCommand();
			if (command.equals("reset")) {

				int opcion = JOptionPane.showConfirmDialog(null,
						"Do you want to reset ?", "Confirmation",
						JOptionPane.YES_NO_OPTION);

				if (opcion == 0) {
					textRandomization.setText(null);
					keyTypeCombo.setSelectedIndex(0);

					for (int i = 0; i < colLength; i++) {
						columnTable.getModel().setValueAt(false, i, 2);
						columnTable.getModel().setValueAt(false, i, 3);
					}

					uniqueNess.setSelected(true);
					possibleValues.setSelected(true);
					dateType.setSelected(true);
					sum.setSelected(true);
					min.setSelected(true);
					max.setSelected(true);
					mean.setSelected(true);
					mode.setSelected(false);
				} else {
				}
			}

			else if (command.equals("submit")) {
				if (textRandomization.getText().isEmpty()
						|| textRandomization.getText() == null) {
					JOptionPane.showMessageDialog(null,
							"Please enter the data randomization percentage.");
				} else {
					boolean isInteger = true;
					try {
						Integer.parseInt(textRandomization.getText());
					} catch (NumberFormatException ex) {
						isInteger = false;
					}

					String keyTypeValue = (String) keyTypeCombo
							.getSelectedItem();
					int noOfSelectedCells = 0;
					for (int i = 0; i < colLength; i++) {
						if (columnTable.getModel().getValueAt(i, 3) != null
								&& columnTable.getModel().getValueAt(i, 3)
										.equals(true)) {
							noOfSelectedCells = noOfSelectedCells + 1;
						}
					}

					if (isInteger == false) {
						JOptionPane
								.showMessageDialog(null,
										"Data randomization percentage must be a number between 0 to 100.");
						textRandomization.setText(null);
					} else if (Integer.parseInt(textRandomization.getText()) <= 0
							|| Integer.parseInt(textRandomization.getText()) > 100) {
						JOptionPane
								.showMessageDialog(null,
										"Data randomization percentage must be between 0 to 100.");
						textRandomization.setText(null);
					} else if (keyTypeValue.equalsIgnoreCase("Primary Key")
							&& noOfSelectedCells == 0) {
						JOptionPane.showMessageDialog(null,
								"Please select primary key column.");
					} else if (keyTypeValue.equalsIgnoreCase("Composit Key")
							&& noOfSelectedCells <= 1) {
						JOptionPane.showMessageDialog(null,
								"Please select all composit key columns.");
					} else {
						boolean checkSelected = false;
						if (uniqueNess.isSelected()
								|| possibleValues.isSelected()
								|| dateType.isSelected() || sum.isSelected()
								|| min.isSelected() || max.isSelected()
								|| mean.isSelected() || mode.isSelected()) {
							checkSelected = true;
						}
						if (checkSelected == false) {
							JOptionPane
									.showMessageDialog(null,
											"Please select quality check operations to perform.");
						} else {
							boolean columnSelected = false;
							for (int i = 0; i < colLength; i++) {
								if (columnTable.getModel().getValueAt(i, 2) != null
										&& columnTable.getModel()
												.getValueAt(i, 2).equals(true)) {
									columnSelected = true;
								}
							}
							if (columnSelected == false) {
								JOptionPane
										.showMessageDialog(null,
												"No column is selected, all columns will be taken by default.");
							}

							int opcion = JOptionPane.showConfirmDialog(null,
									"Do you want to submit ?", "Confirmation",
									JOptionPane.YES_NO_OPTION);

							if (opcion == 0) {
								final ConfigurationDetailsBean configurationDetailsBean = new ConfigurationDetailsBean();

								configurationDetailsBean
										.setUniquenessRule(false);
								configurationDetailsBean
										.setPossibleValueRule(false);
								configurationDetailsBean.setDateTypeRule(false);
								configurationDetailsBean
										.setSummationRule(false);
								configurationDetailsBean.setMinimumRule(false);
								configurationDetailsBean.setMaximumRule(false);
								configurationDetailsBean.setMeanRule(false);
								configurationDetailsBean.setModeRule(false);

								configurationDetailsBean
										.setRandomizationPrecentage(Integer
												.parseInt(textRandomization
														.getText()));

								boolean added = false;
								ArrayList<String> selectedColList = new ArrayList<String>();
								for (int i = 0; i < colLength; i++) {
									if (columnTable.getModel().getValueAt(i, 2) != null
											&& columnTable.getModel()
													.getValueAt(i, 2)
													.equals(true)) {
										selectedColList.add(columnTable
												.getModel().getValueAt(i, 0)
												.toString());
										added = true;
									}
								}
								if (added == false) {
									for (int i = 0; i < colLength; i++) {
										selectedColList.add(columnTable
												.getModel().getValueAt(i, 0)
												.toString());
									}
								}
								configurationDetailsBean
										.setColumnNames(selectedColList);

								if (uniqueNess.isSelected()) {
									configurationDetailsBean
											.setUniquenessRule(true);
								}
								if (possibleValues.isSelected()) {
									configurationDetailsBean
											.setPossibleValueRule(true);
								}
								if (dateType.isSelected()) {
									configurationDetailsBean
											.setDateTypeRule(true);
								}
								if (sum.isSelected()) {
									configurationDetailsBean
											.setSummationRule(true);
								}
								if (min.isSelected()) {
									configurationDetailsBean
											.setMinimumRule(true);
								}
								if (max.isSelected()) {
									configurationDetailsBean
											.setMaximumRule(true);
								}
								if (mean.isSelected()) {
									configurationDetailsBean.setMeanRule(true);
								}
								if (mode.isSelected()) {
									configurationDetailsBean.setModeRule(true);
								}

								configurationDetailsBean
										.setKeyType((String) keyTypeCombo
												.getSelectedItem());

								ArrayList<String> keyColList = new ArrayList<String>();
								for (int i = 0; i < colLength; i++) {
									if (columnTable.getModel().getValueAt(i, 3) != null
											&& columnTable.getModel()
													.getValueAt(i, 3)
													.equals(true)) {
										keyColList.add(columnTable.getModel()
												.getValueAt(i, 0).toString());
									}
								}
								configurationDetailsBean
										.setKeyColumnNames(keyColList);

								mainFrame.setVisible(false);

								BackgroundOperationWait backgroundOperationWait = new BackgroundOperationWait();
								backgroundOperationWait.runWaitBar(
										ConnDetailsBean,
										configurationDetailsBean);
							} else {
							}
						}

					}
				}

			} else if (command.equals("keyType")) {
				for (int i = 0; i < colLength; i++) {
					columnTable.getModel().setValueAt(false, i, 3);
				}
			} else if (command.equals("back")) {
				int opcion = JOptionPane.showConfirmDialog(null,
						"Do you want to go back ?", "Confirmation",
						JOptionPane.YES_NO_OPTION);

				if (opcion == 0) {
					mainFrame.setVisible(false);
					HomePage obj = new HomePage(ConnDetailsBean);
					obj.showEventDemo();
				} else {
				}

			}
		}
	}
}
