package com.java.main.ui;

public class ConnectionDetailsBean {

	private boolean csvSource;

	private String srcFileAbsolutePath;
	private String destFileAbsolutePath;

	public boolean isCsvSource() {
		return csvSource;
	}

	public void setCsvSource(boolean csvSource) {
		this.csvSource = csvSource;
	}

	public String getSrcFileAbsolutePath() {
		return srcFileAbsolutePath;
	}

	public void setSrcFileAbsolutePath(String srcFileAbsolutePath) {
		this.srcFileAbsolutePath = srcFileAbsolutePath;
	}

	public String getDestFileAbsolutePath() {
		return destFileAbsolutePath;
	}

	public void setDestFileAbsolutePath(String destFileAbsolutePath) {
		this.destFileAbsolutePath = destFileAbsolutePath;
	}

}
