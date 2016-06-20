package com.java.main.exceptions;

public class NullCellException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4173577496436022560L;

	public NullCellException() {
		super();
	}

	public NullCellException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	public NullCellException(String message, Throwable cause) {
		super(message, cause);
	}

	public NullCellException(String message) {
		super(message);
	}

	public NullCellException(Throwable cause) {
		super(cause);
	}
	
	

}
