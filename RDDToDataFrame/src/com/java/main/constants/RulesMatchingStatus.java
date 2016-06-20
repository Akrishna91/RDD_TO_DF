package com.java.main.constants;

public enum RulesMatchingStatus {
	/**
	 * if rules are perfectly matched
	 */
	MATCHED,
	
	/**
	 * if there is no match in rules
	 */
	MISMATCHED,
	
	/**
	 * if there is a possibility of match
	 */
	MIGHT_MATCH;
}
