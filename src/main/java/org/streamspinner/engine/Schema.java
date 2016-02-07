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
package org.streamspinner.engine;

import org.streamspinner.DataTypes;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.query.AttributeList;
import org.streamspinner.query.Attribute;
import org.streamspinner.query.RenameParameter;
import java.io.Serializable;
import java.sql.Types;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Schema implements Serializable {

	/**
	 * @deprecated replaced by {@link DataTypes#STRING}
	 */
	public static final String STRING = DataTypes.STRING;

	/**
	 * @deprecated replaced by {@link DataTypes#LONG}
	 */
	public static final String LONG = DataTypes.LONG;

	/**
	 * @deprecated replaced by {@link DataTypes#DOUBLE}
	 */
	public static final String DOUBLE = DataTypes.DOUBLE;

	/**
	 * @deprecated replaced by {@link DataTypes#OBJECT}
	 */
	public static final String OBJECT = DataTypes.OBJECT;

	/**
	 * @deprecated replaced by {@link DataTypes#ARRAY_STRING}
	 */
	public static final String ARRAY_STRING = DataTypes.ARRAY_STRING;

	/**
	 * @deprecated replaced by {@link DataTypes#ARRAY_LONG}
	 */
	public static final String ARRAY_LONG = DataTypes.ARRAY_LONG;

	/**
	 * @deprecated replaced by {@link DataTypes#ARRAY_DOUBLE}
	 */
	public static final String ARRAY_DOUBLE = DataTypes.ARRAY_DOUBLE;

	/**
	 * @deprecated replaced by {@link DataTypes#ARRAY_OBJECT}
	 */
	public static final String ARRAY_OBJECT = DataTypes.ARRAY_OBJECT;

	public static final String RDB="RDB";
	public static final String STREAM="STREAM";
	public static final String HISTORY="HISTORY";
	public static final String UNKNOWN="UNKNOWN";

	private String[] attributes;
	private String[] types;
	private String tabletype;
	private String[] tablenames;

	public Schema(){
		attributes = new String[0];
		types = new String[0];
		tabletype = UNKNOWN;
		tablenames = new String[0];
	}


	public Schema(String tablename, String[] attributenames, String[] types) {
		if(types == null || attributenames == null || types.length != attributenames.length)
			throw new IllegalArgumentException("Arguments are invalid.");
		this.attributes = attributenames;
		this.types = types;
		this.tabletype = UNKNOWN;
		this.tablenames = new String[1];
		this.tablenames[0] = tablename;
	}

	public Schema(ResultSetMetaData rsmd) throws StreamSpinnerException {
		try {
			int count = rsmd.getColumnCount();
			attributes = new String[count];
			types = new String[count];
			HashSet names = new HashSet();
			for(int i=0; i < count; i++){
				String tablename = rsmd.getTableName(i+1);
				names.add(tablename);
				attributes[i] = tablename + "." + rsmd.getColumnName(i+1);
				types[i] = DataTypes.convertSQLType(rsmd.getColumnType(i+1));
			}
			tabletype = RDB;
			tablenames = (String[])(names.toArray(new String[0]));
		} catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public Schema concat(Schema target){
		if(target == null)
			throw new IllegalArgumentException("argument is invalid.");

		Schema rval = new Schema();

		ArrayList attrlist = new ArrayList(Arrays.asList(this.attributes));
		attrlist.addAll(Arrays.asList(target.attributes));
		rval.attributes = (String[])(attrlist.toArray(new String[0]));

		ArrayList typelist = new ArrayList(Arrays.asList(this.types));
		typelist.addAll(Arrays.asList(target.types));
		rval.types = (String[])(typelist.toArray(new String[0]));

		rval.tabletype = UNKNOWN;

		HashSet names = new HashSet(Arrays.asList(this.tablenames));
		names.addAll(Arrays.asList(target.tablenames));
		rval.tablenames = (String[])(names.toArray(new String[0]));

		return rval;
	}

	public Schema subset(AttributeList attrs) throws StreamSpinnerException {
		if(attrs.size() == 1 && attrs.getString(0).equals("*"))
			return this;

		Schema rval = new Schema();
		rval.attributes = new String[attrs.size()];
		rval.types = new String[attrs.size()];

		for(int i=0; i < attrs.size(); i++){
			if(! contains(attrs.getString(i)))
				throw new StreamSpinnerException("Attribute not found: " + attrs.getString(i));
			rval.attributes[i] = attributes[getIndex(attrs.getString(i))];
			rval.types[i] = types[getIndex(attrs.getString(i))];
		}
		rval.tabletype = UNKNOWN;
		rval.tablenames = tablenames;
		return rval;
	}

	public Schema append(String attributename, String type){
		Schema rval = new Schema();

		rval.attributes = new String[attributes.length + 1];
		System.arraycopy(attributes, 0, rval.attributes, 0, attributes.length);
		rval.attributes[rval.attributes.length -1] = attributename;

		rval.types = new String[types.length + 1];
		System.arraycopy(types, 0, rval.types, 0, types.length);
		rval.types[rval.types.length -1] = type;

		rval.tabletype = UNKNOWN;
		rval.tablenames = new String[tablenames.length];
		System.arraycopy(tablenames, 0, rval.tablenames, 0, tablenames.length);
		return rval;
	}

	public Schema group(AttributeList key){
		Schema rval = copy();
		for(int i=0; i < rval.size(); i++){
			boolean found = false;
			for(int j=0; j < key.size(); j++){
				if(rval.getAttributeName(i).equals(key.getString(j)))
					found = true;
			}
			if(found == false)
				rval.types[i] = DataTypes.toArrayType(rval.getType(i));
		}
		return rval;
	}

	public Schema rename(RenameParameter rp){
		Schema rval = copy();
		AttributeList newattrs = rp.rename(getAttributeList());
		for(int i=0; i < newattrs.size(); i++)
			rval.attributes[i] = newattrs.getString(i);
		return rval;
	}

	public String[] getAttributeNames(){
		return attributes;
	}

	public String getAttributeName(int i){
		if(i < 0 || i >= attributes.length)
			throw new ArrayIndexOutOfBoundsException("index = " + i);
		return attributes[i];
	}

	public AttributeList getAttributeList(){
		return new AttributeList(attributes);
	}

	public String[] getTypes(){
		return types;
	}

	public String getType(String attributename) {
		for(int i=0; i < attributes.length; i++)
			if(attributes[i].equals(attributename))
				return types[i];
		throw new IllegalArgumentException("Attribute is not found");
	}


	public String getType(int i){
		if(i < 0 || i >= attributes.length)
			throw new ArrayIndexOutOfBoundsException("index = " + i);
		return types[i];
	}


	public boolean contains(String attributename){
		for(int i=0; i < attributes.length; i++){
			if(attributes[i].equals(attributename))
				return true;
		}
		return false;
	}

	public Schema copy(){
		Schema rval = new Schema();

		rval.attributes = new String[attributes.length];
		System.arraycopy(attributes, 0, rval.attributes, 0, attributes.length);
		rval.types = new String[types.length];
		System.arraycopy(types, 0, rval.types, 0, types.length);
		rval.tablenames= new String[tablenames.length];
		System.arraycopy(tablenames, 0, rval.tablenames, 0, tablenames.length);
		rval.tabletype = tabletype;
		return rval;
	}

	public int getIndex(String attribute){
		for(int i=0; i < attributes.length; i++){
			if(attributes[i].equals(attribute))
				return i;
		}
		throw new IllegalArgumentException("AttributeName '"+attribute+"' is not found");
	}

	public int size(){
		return attributes.length;
	}

	public String getTableType(){
		return tabletype;
	}

	public void setTableType(String type){
		if(type.equals(RDB) || type.equals(STREAM) || type.equals(HISTORY))
			tabletype = type;
		else
			tabletype = UNKNOWN;
	}

	public String[] getBaseTableNames(){
		return tablenames;
	}

	public Schema renameTableName(String from, String to){
		Schema rval = copy();
		if(from == null || to == null || from.equals(to))
			return rval;

		AttributeList oldattr = new AttributeList(rval.attributes);
		AttributeList newattr = oldattr.renameSourceName(from, to);
		for(int i=0; i < rval.attributes.length; i++)
			rval.attributes[i] = newattr.getString(i);

		for(int i=0; i < rval.tablenames.length; i++){
			if(rval.tablenames[i].equals(from))
				rval.tablenames[i] = to;
		}
		return rval;
	}

	public Schema renameColumnName(String from, String to){
		Schema rval = copy();
		if(from == null || to == null || from.equals(to))
			return rval;
		AttributeList oldattr = new AttributeList(rval.attributes);
		AttributeList newattr = oldattr.renameColumnName(from, to);
		for(int i=0; i < rval.attributes.length; i++)
			rval.attributes[i] = newattr.getString(i);
		return rval;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		if(tablenames.length == 1)
			sb.append(tablenames[0]);
		else {
			sb.append("JoinOf");
			for(int i=0; i < tablenames.length; i++){
				sb.append(tablenames[i]);
				if(i + 1 < tablenames.length)
					sb.append("And");
			}
		}
		sb.append("(");
		for(int i=0; i < size(); i++){
			sb.append(getAttributeName(i));
			sb.append(" ");
			sb.append(getType(i));
			if(i + 1 < size())
				sb.append(", ");
		}
		sb.append(")");
		return sb.toString();
	}

	public boolean equals(Object obj){
		if(! (obj instanceof Schema))
			return false;
		Schema target = (Schema)obj;
		if(! Arrays.equals(attributes, target.attributes))
			return false;
		if(! Arrays.equals(types, target.types))
			return false;
		if(! Arrays.equals(tablenames, target.tablenames))
			return false;
		if(! tabletype.equals(target.tabletype))
			return false;
		return true;
	}

	public static Schema parse(String schemastr) throws StreamSpinnerException {
		Pattern entire = Pattern.compile("^\\s*(\\w+)\\((\\s*.+\\s+.+\\s*,?)+\\s*\\)\\s*$");
		Matcher em = entire.matcher(schemastr);
		if(! em.find())
			throw new StreamSpinnerException("Can not parse string : " + schemastr);
		String tablename = em.group(1);
		String remain = em.group(2);

		ArrayList<String> attrs = new ArrayList<String>();
		ArrayList<String> types = new ArrayList<String>();

		Pattern attrtype = Pattern.compile("\\s*([\\w\\.]+)\\s+([\\w\\.]+)\\s*,?");
		Matcher atm = attrtype.matcher(remain);
		int index = 0;
		while(atm.find(index)){
			String attribute = atm.group(1);
			if(attribute.indexOf(".") < 0)
				attribute = tablename + "." + attribute;

			String type = atm.group(2);
			if(! DataTypes.isDataTypeString(type))
				throw new StreamSpinnerException("Unknown type: " + type);

			attrs.add(attribute);
			types.add(type);
			index = atm.end();
		}
		return new Schema(tablename, attrs.toArray(new String[0]), types.toArray(new String[0]));
	}
}

