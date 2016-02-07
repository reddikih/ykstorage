/*
 * Copyright 2005-2009 StreamSpinner Project
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.streamspinner;

import java.sql.Types;
import java.util.Arrays;

public abstract class DataTypes {

	public static final String STRING="String";

	public static final String LONG="Long";

	public static final String DOUBLE="Double";

	public static final String OBJECT="Object";

	public static final String ARRAY_STRING="String[]";

	public static final String ARRAY_LONG="Long[]";

	public static final String ARRAY_DOUBLE="Double[]";

	public static final String ARRAY_OBJECT="Object[]";

	public static String convertSQLType(int type) throws StreamSpinnerException {
		switch(type){
			case Types.BIGINT:
			case Types.INTEGER:
			case Types.SMALLINT:
			case Types.TINYINT:
			case Types.DECIMAL:
				return LONG;
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.REAL:
				return DOUBLE;
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
			case Types.CHAR:
				return STRING;
			case Types.ARRAY:
				return ARRAY_OBJECT;
			case Types.OTHER:
				return OBJECT;
			default:
				throw new StreamSpinnerException("Unknown type");
		}
	}

	public static boolean isDataTypeString(String str){
		String[] target = { STRING, LONG, DOUBLE, OBJECT, ARRAY_STRING, ARRAY_LONG, ARRAY_DOUBLE, ARRAY_OBJECT };
		for(int i=0; i < target.length; i++)
			if(str.equals(target[i]))
				return true;
		return false;
	}


	public static Class getClass(String typename) throws StreamSpinnerException {
		if(typename.equals(STRING))
			return String.class;
		else if(typename.equals(LONG))
			return long.class;
		else if(typename.equals(DOUBLE))
			return double.class;
		else if(typename.equals(OBJECT))
			return Object.class;
		else if(typename.equals(ARRAY_STRING))
			return String[].class;
		else if(typename.equals(ARRAY_LONG))
			return long[].class;
		else if(typename.equals(ARRAY_DOUBLE))
			return double[].class;
		else if(typename.equals(ARRAY_OBJECT))
			return Object[].class;
		else 
			throw new StreamSpinnerException("Unknown type : " + typename);
	}

	public static boolean isArray(String typename) {
		return typename.endsWith("[]");
	}

	public static String toArrayType(String basetype) {
		if(basetype.equals(STRING))
			return ARRAY_STRING;
		else if(basetype.equals(LONG))
			return ARRAY_LONG;
		else if(basetype.equals(DOUBLE))
			return ARRAY_DOUBLE;
		else 
			return ARRAY_OBJECT;
	}

	public static String toString(Object obj) {
		if(obj instanceof Object[])
			return Arrays.deepToString((Object[])obj);
		else if(obj instanceof long[])
			return Arrays.toString((long[])obj);
		else if(obj instanceof double[])
			return Arrays.toString((double[])obj);
		else if(obj != null)
			return obj.toString();
		return "";
	}
}

