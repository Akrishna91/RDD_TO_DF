package com.java.main.associationrules.src;

import java.util.List;

import org.apache.spark.mllib.fpm.AssociationRules.Rule;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;

public class AssociationRule {

	private List<FreqItemset<String>> freqItems;
	private Object assocRules;

	public AssociationRule(List<FreqItemset<String>> freqItems,
			List<Rule<String>> assocRules) {
		this.freqItems = freqItems;
		this.assocRules = assocRules;
	}

	public List<FreqItemset<String>> getFreqItems() {
		return freqItems;
	}

	public void setFreqItems(List<FreqItemset<String>> freqItems) {
		this.freqItems = freqItems;
	}

	public Object getAssocRules() {
		return assocRules;
	}

	public void setAssocRules(Object assocRules) {
		this.assocRules = assocRules;
	}

}
