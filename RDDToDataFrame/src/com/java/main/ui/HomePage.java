package com.java.main.ui;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

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
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import com.java.main.context.GetJavaSparkContext;


/**
* 
 * 
 * 
 * @author kbaghel Description - This class is used to generate home page
*/

public class HomePage {

       private JFrame mainFrame;
       private JLabel headerLabel, fileLable1, fileLable2;
       private JPanel basePanel, filePanel, databasePanel, srcPanel, destPanel, topPanel, bottomPanel, formatPanel;
       private java.io.File srcFile, destFile;
       private JButton showSrcFileDialogButton, showDestFileDialogButton;
       private JRadioButton csvButton, parquetButton;
       private JLabel srcParquetPath, destParquetPath;
       private JTextField textSrcParquetPath, textDestParquetPath;
       ConnectionDetailsBean connDtlsBean = null;

       /**
       * 
        * Constructor
       */

       public HomePage() {
             prepareGUI();
       }

       /**
       * 
        * Constructor with parameters used for back operation
       * 
        * 
        * 
        * @param ConnBean
       */

       public HomePage(ConnectionDetailsBean ConnBean) {
             connDtlsBean = ConnBean;
             prepareGUI();
       }

       /**
       * 
        * main method to start application
       * 
        * 
        * 
        * @param args
       */

       public static void main(String[] args) {

             // Calling context loading thread - Start

             final Thread doContextLoad = new Thread(new Runnable() {

                    public void run() {

                           try {
                                 GetJavaSparkContext.getJavaSparkContex();
                           } catch (Exception ex) {
                                 ex.printStackTrace();
                           }

                           System.out.println("Context load completed!");

                    }

             });

             // Starting Thread

             doContextLoad.start();

             // Calling context loading thread - End

             HomePage swingControlDemo = new HomePage();

             swingControlDemo.showEventDemo();

       }

       private void prepareGUI() {

             mainFrame = new JFrame("Systemic Data Fabric Validator");

             mainFrame.setSize(1920, 1030);

             mainFrame.setLocation(0, 0);

             mainFrame.addWindowListener(new WindowAdapter() {

                    public void windowClosing(WindowEvent windowEvent) {

                           System.exit(0);

                    }

             });

             JLabel background = new JLabel(new ImageIcon(Path.getImagePath()));

             background.setLayout(new BoxLayout(background, BoxLayout.PAGE_AXIS));

             // Initializing Labels

             headerLabel = new JLabel("", JLabel.CENTER);

             fileLable1 = new JLabel("", JLabel.CENTER);

             fileLable2 = new JLabel("", JLabel.CENTER);

             srcParquetPath = new JLabel("", JLabel.HORIZONTAL);

             destParquetPath = new JLabel("", JLabel.CENTER);

             // Initializing panels

             basePanel = new JPanel();
             basePanel.setLayout(new BoxLayout(basePanel, BoxLayout.LINE_AXIS));
             basePanel.setLocation(10, 0);
             basePanel.setPreferredSize(new Dimension(100, 1000));
             basePanel.setBackground(Color.WHITE);

             formatPanel = new JPanel();
             formatPanel.setLayout(new BoxLayout(formatPanel, BoxLayout.PAGE_AXIS));
             formatPanel.setPreferredSize(new Dimension(1900, 600));
             formatPanel.setBackground(Color.WHITE);

             topPanel = new JPanel();
             topPanel.setLayout(new FlowLayout());
             topPanel.setPreferredSize(new Dimension(1900, 150));
             topPanel.setBorder(BorderFactory
             .createTitledBorder("Select Source of Data"));
             topPanel.setBackground(Color.WHITE);

             bottomPanel = new JPanel();
             bottomPanel.setLayout(new FlowLayout());
             bottomPanel.setPreferredSize(new Dimension(1900, 150));
             bottomPanel.setBackground(Color.WHITE);

             filePanel = new JPanel();
             filePanel.setLayout(new FlowLayout());
             filePanel.setPreferredSize(new Dimension(300, 150));
             filePanel.setBorder(BorderFactory.createTitledBorder("CSV Files"));
             filePanel.setBackground(Color.WHITE);

             databasePanel = new JPanel();
             databasePanel.setLayout(new BoxLayout(databasePanel,
             BoxLayout.PAGE_AXIS));
             databasePanel.setPreferredSize(new Dimension(700, 650));
             databasePanel.setBorder(BorderFactory.createTitledBorder("Parquet Path"));
             databasePanel.setBackground(Color.WHITE);

             srcPanel = new JPanel();
             srcPanel.setLayout(new GridLayout(0, 2));
             srcPanel.setPreferredSize(new Dimension(1200, 150));
             //srcPanel.setBorder(BorderFactory
             //.createTitledBorder("Source Parquet Details"));
             srcPanel.setBackground(Color.WHITE);

             destPanel = new JPanel();
             destPanel.setLayout(new GridLayout(0, 2));
             destPanel.setPreferredSize(new Dimension(1200, 200));
             //destPanel.setBorder(BorderFactory
             //.createTitledBorder("Destination Parquet Details"));
             destPanel.setBackground(Color.WHITE);

             // Adding all components

             mainFrame.add(background);

             background.add(Box.createRigidArea(new Dimension(0, 150)));
             background.add(headerLabel);

             databasePanel.add(Box.createRigidArea(new Dimension(0, 30)));
             databasePanel.add(srcPanel);
             databasePanel.add(Box.createRigidArea(new Dimension(0, 30)));
             databasePanel.add(destPanel);
             databasePanel.add(Box.createRigidArea(new Dimension(0, 80)));

             formatPanel.add(topPanel);
             formatPanel.add(filePanel);
             formatPanel.add(databasePanel);
             formatPanel.add(Box.createRigidArea(new Dimension(0, 250)));
             formatPanel.add(bottomPanel);
             
             basePanel.add(Box.createRigidArea(new Dimension(300, 0)));
             basePanel.add(formatPanel);
             basePanel.add(Box.createRigidArea(new Dimension(300, 0)));

             background.add(basePanel);
             background.add(Box.createRigidArea(new Dimension(0, 50)));

             mainFrame.setVisible(true);

       }

       public void showEventDemo() {

             headerLabel.setText(" ");

             // File chooser for source and destination files

             final JFileChooser srcFileDialog = new JFileChooser();

             showSrcFileDialogButton = new JButton("Select Source File");

             showSrcFileDialogButton.addActionListener(new ActionListener() {

                    public void actionPerformed(ActionEvent e) {

                           int returnVal = srcFileDialog.showOpenDialog(mainFrame);

                           if (returnVal == JFileChooser.APPROVE_OPTION) {

                                 srcFile = srcFileDialog.getSelectedFile();

                                 fileLable1.setText("Source File Selected : "

                                 + srcFile.getName());

                           } else {

                                 // fileLable1.setText("Open command cancelled by user.");

                           }

                    }

             });

             final JFileChooser destFileDialog = new JFileChooser();

             showDestFileDialogButton = new JButton("Select Destination File");

             showDestFileDialogButton.addActionListener(new ActionListener() {

                    public void actionPerformed(ActionEvent e) {

                           int returnVal = destFileDialog.showOpenDialog(mainFrame);

                           if (returnVal == JFileChooser.APPROVE_OPTION) {

                                 destFile = destFileDialog.getSelectedFile();

                                 fileLable2.setText("Destination File Selected : "

                                 + destFile.getName());

                           } else {

                                 // fileLable2.setText("Open command cancelled by user.");

                           }

                    }

             });

             showSrcFileDialogButton.setEnabled(true);

             showDestFileDialogButton.setEnabled(true);

             srcParquetPath.setText("Source Path");
             destParquetPath.setText("Destination Path");

             textSrcParquetPath = new JTextField(100);
             textSrcParquetPath.setVisible(true);
             textSrcParquetPath.setEnabled(false);

             textDestParquetPath = new JTextField(100);
             textDestParquetPath.setVisible(true);
             textDestParquetPath.setEnabled(false);

             csvButton = new JRadioButton("CSV");
             csvButton.setActionCommand("csv");
             csvButton.setSelected(true);

             parquetButton = new JRadioButton("Parquet");
             parquetButton.setActionCommand("parquet");
             parquetButton.setSelected(false);

             JButton resetButton = new JButton("Reset");
             JButton submitButton = new JButton("Next");
             JButton closeButton = new JButton("Close");

             resetButton.setActionCommand("reset");
             submitButton.setActionCommand("next");
             closeButton.setActionCommand("close");

             csvButton.addActionListener(new ButtonClickListener());
             parquetButton.addActionListener(new ButtonClickListener());

             resetButton.addActionListener(new ButtonClickListener());
             submitButton.addActionListener(new ButtonClickListener());
             closeButton.addActionListener(new ButtonClickListener());

             filePanel.add(showSrcFileDialogButton);
             filePanel.add(fileLable1);
             filePanel.add(showDestFileDialogButton);
             filePanel.add(fileLable2);

             topPanel.add(csvButton);
             topPanel.add(parquetButton);

             srcPanel.add(srcParquetPath);
             srcPanel.add(textSrcParquetPath);

             destPanel.add(destParquetPath);
             destPanel.add(textDestParquetPath);

             bottomPanel.add(submitButton);
             bottomPanel.add(resetButton);
             bottomPanel.add(closeButton);

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

                                 csvButton.setSelected(true);
                                 parquetButton.setSelected(false);
                                 
                                 srcFile = null;
                                 destFile = null;

                                 showSrcFileDialogButton.setEnabled(true);
                                 showDestFileDialogButton.setEnabled(true);

                                 fileLable1.setText("");
                                 fileLable2.setText("");

                                 textSrcParquetPath.setText(null);
                                 textSrcParquetPath.setEnabled(false);

                                 textDestParquetPath.setText(null);
                                 textDestParquetPath.setEnabled(false);

                           } else {

                           }

                    } else if (command.equals("csv")) {

                           csvButton.setSelected(true);
                           parquetButton.setSelected(false);

                           showSrcFileDialogButton.setEnabled(true);
                           showDestFileDialogButton.setEnabled(true);

                           fileLable1.setText("");
                           fileLable2.setText("");

                           textSrcParquetPath.setText(null);
                           textSrcParquetPath.setEnabled(false);

                           textDestParquetPath.setText(null);
                           textDestParquetPath.setEnabled(false);

                    } else if (command.equals("parquet")) {

                           csvButton.setSelected(false);
                           parquetButton.setSelected(true);

                           showSrcFileDialogButton.setEnabled(false);
                           showDestFileDialogButton.setEnabled(false);

                           fileLable1.setText("");
                           fileLable2.setText("");

                           textSrcParquetPath.setText(null);
                           textSrcParquetPath.setEnabled(true);

                           textDestParquetPath.setText(null);
                           textDestParquetPath.setEnabled(true);

                    } else if (command.equals("next")) {

                           boolean status = false;

                           if (csvButton.isSelected()) {

                                 if (fileLable1.getText().equals("") && srcFile == null) {

                                        JOptionPane.showMessageDialog(null,

                                        "Please select source file.");

                                 } else if (fileLable2.getText().equals("")

                                 && destFile == null) {

                                        JOptionPane.showMessageDialog(null,

                                        "Please select destination file.");

                                 } else {
                                        status = true;
                                 }
                           }

                           if (parquetButton.isSelected()) {

                                 if (textSrcParquetPath.getText().equals("")) {
                                        JOptionPane.showMessageDialog(null,
                                                     "Please enter parquet source path.");
                                 } else if (textDestParquetPath.getText().equals("")) {
                                        JOptionPane.showMessageDialog(null,
                                                     "Please enter parquet destination path.");
                                 } else {
                                        status = true;
                                 }
                           }

                           if (status == true) {

                                 ConnectionDetailsBean connectionDetailsBean = new ConnectionDetailsBean();

                                 if (csvButton.isSelected()) {

                                        connectionDetailsBean.setCsvSource(true);
                                        connectionDetailsBean.setSrcFileAbsolutePath(srcFile
                                                     .getAbsolutePath());
                                        connectionDetailsBean.setDestFileAbsolutePath(destFile
                                                     .getAbsolutePath());

                                 } else {

                                        connectionDetailsBean.setCsvSource(false);
                                        connectionDetailsBean
                                                      .setSrcFileAbsolutePath(textSrcParquetPath.getText());
                                        connectionDetailsBean
                                                      .setDestFileAbsolutePath(textSrcParquetPath
                                                                   .getText());

                                 }

                                 mainFrame.setVisible(false);

                                 DataCollectionWait DataCollectionWait = new
                                 DataCollectionWait();
                                  DataCollectionWait.runWaitBar(connectionDetailsBean);

                           }

                    } else {

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

