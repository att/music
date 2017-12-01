package com.att.research.mdbc;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Information about a table in the local database.  It consists of three ordered list, which should all have the
 * same length. A list of column names, a list of DB column types, and a list of booleans specifying which columns are keys.
 * @author Robert P. Eby
 */
public class TableInfo {
	/** An ordered list of the column names in this table */
	public List<String>  columns;
	/** An ordered list of the column types in this table; the types are integers taken from {@link java.sql.Types}. */
	public List<Integer> coltype;
	/** An ordered list of booleans indicating if a column is a primary key column or not. */
	public List<Boolean> iskey;

	/** Construct an (initially) empty TableInfo. */
	public TableInfo() {
		columns  = new ArrayList<String>();
		coltype  = new ArrayList<Integer>();
		iskey    = new ArrayList<Boolean>();
	}
	/**
	 * Check whether the column whose name is <i>name</i> is a primary key column.
	 * @param name the column name
	 * @return true if it is, false otherwise
	 */
	public boolean iskey(String name) {
		for (int i = 0; i < columns.size(); i++) {
			if (this.columns.get(i).equalsIgnoreCase(name))
				return this.iskey.get(i);
		}
		return false;
	}
	/**
	 * Get the type of the column whose name is <i>name</i>.
	 * @param name the column name
	 * @return the column type or Types.NULL
	 */
	public int getColType(String name) {
		for (int i = 0; i < columns.size(); i++) {
			if (this.columns.get(i).equalsIgnoreCase(name))
				return this.coltype.get(i);
		}
		return Types.NULL;
	}
}
