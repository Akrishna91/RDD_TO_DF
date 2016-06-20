package com.java.main.beans;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public class ColSummary {
	private Double min, max;
	private String type;
	private LinkedHashSet<String> levels;
	private LinkedHashSet<String> formats;
	private Integer cardinality;

	public Double getMin() {
		return min;
	}

	public void setMin(Double min) {
		this.min = min;
	}

	public Double getMax() {
		return max;
	}

	public void setMax(Double max) {
		this.max = max;
	}

	public int getCardinality() {
		return cardinality;
	}

	public void setCardinality(int cardinality) {
		this.cardinality = cardinality;
	}

	public LinkedHashSet<String> getLevels() {
		return levels;
	}

	public void setLevels(LinkedHashSet<String> levels) {
		this.levels = levels;
		if(levels != null){
			this.cardinality = levels.size();
		}
		
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String toString(){
		String summary = "\n";
		summary += "Column Type: " + this.type + "\n";
		if (this.type.toLowerCase().equals("numeric")){
			summary += "Max: " + this.max + "\n";
			summary += "Min: " + this.min + "\n";
		}else if(this.type.toLowerCase().equals("date")){
			summary += "Possible Formats: " + this.formats;
		}else{
			summary += "cardinality: " + this.cardinality + "\n";
			summary += "Levels: ";
			int numLevels = 0 ;
			for(String level : levels){
				summary += level + " ";
				if(numLevels > 10){
					summary += ".....\n";
					break;
				}
				numLevels++;
			}
		}
		return summary;
	}
	
	public HashMap<String, String> getSummary(){
		HashMap<String, String> smry = new LinkedHashMap<String, String>();
		
		String summary = "";
		summary += "Column Type: " + this.type + "\n";
		smry.put("Column Type", this.type);
		if (this.type.toLowerCase().equals("numeric")){
			summary += "Max: " + this.max + "\n";
			summary += "Min: " + this.min + "\n";
			smry.put("Max", this.max.toString());
			smry.put("Min", this.min.toString());
		}else if(this.type.toLowerCase().equals("date")){
			summary += "Possible Formats: " + this.formats;
			smry.put("Possible Formates", this.formats.toString());
		}else{
			summary += "cardinality: " + this.cardinality + "\n";
			smry.put("cardinality", this.cardinality.toString());
			summary += "Levels: ";
			int numLevels = 0 ;
			String tempLevel = "";
			for(String level : levels){
				summary += level + ", ";
				tempLevel += level + ", ";
				if(numLevels > 10){
					summary += ".....\n";
					tempLevel += level + ", ";
					break;
				}
				numLevels++;
			}
			smry.put("Levels", tempLevel);
		}
		return smry;
		
	}

	public LinkedHashSet<String> getFormats() {
		return formats;
	}

	public void setFormats(LinkedHashSet<String> formats) {
		this.formats = formats;
	}
}
