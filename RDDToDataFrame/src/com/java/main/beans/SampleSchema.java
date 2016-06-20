package com.java.main.beans;

import java.io.Serializable;

public class SampleSchema implements Serializable {

	private int _id;
	private String name;
	public int get_id() {
		return _id;
	}
	public void set_id(int _id) {
		this._id = _id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

}
