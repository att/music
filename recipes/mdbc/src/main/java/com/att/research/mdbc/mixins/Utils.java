package com.att.research.mdbc.mixins;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Date;

import com.datastax.driver.core.utils.Bytes;

/**
 * Utility functions used by several of the mixins should go here.
 *
 * @author Robert P. Eby
 */
public class Utils {
	/**
	 * Return a String equivalent of an Object.  Useful for writing SQL.
	 * @param val the object to String-ify
	 * @return the String value
	 */
	public static String getStringValue(Object val) {
		if (val == null)
			return "NULL";
		if (val instanceof String)
			return "'" + val.toString().replaceAll("'", "''") + "'";	// double any quotes
		if (val instanceof Number)
			return ""+val;
		if (val instanceof ByteBuffer)
			return "'" + Bytes.toHexString((ByteBuffer)val).substring(2) + "'";	// substring(2) is to remove the "0x" at front
		if (val instanceof Date)
			return "'" + (new Timestamp(((Date)val).getTime())).toString() + "'";
		// Boolean, and anything else
		return val.toString();
	}
}
