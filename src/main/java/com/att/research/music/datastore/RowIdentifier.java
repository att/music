package com.att.research.music.datastore;

public class RowIdentifier{
	public String primarKeyValue;
	public String rowIdString;//the string with all the row identifiers separted by AND
	public RowIdentifier(String primaryKeyValue, String rowIdString){
		this.primarKeyValue = primaryKeyValue;
		this.rowIdString = rowIdString;
	}
}
