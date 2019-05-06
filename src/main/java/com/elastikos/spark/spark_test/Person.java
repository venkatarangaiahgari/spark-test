package com.elastikos.spark.spark_test;

import java.io.Serializable;

public class Person implements Serializable{
	
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6748057261778824081L;
	
	
	private String firstName;
	private String lastName;
	
	public Person(String firstName, String lastName) {
		super();
		this.firstName = firstName;
		this.lastName = lastName;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	
	@Override
	public String toString() {
		return "Person [firstName=" + firstName + ", lastName=" + lastName + "]";
	}
	
	

}
