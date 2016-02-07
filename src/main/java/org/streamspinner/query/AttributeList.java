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
package org.streamspinner.query;

import org.streamspinner.StreamSpinnerException;
import java.util.ArrayList;
import java.util.HashSet;
import java.io.Serializable;

public class AttributeList implements Serializable {

	private ArrayList attr;

	public AttributeList(){
		attr = new ArrayList();
	}

	public AttributeList(String attrstr){
		attr = new ArrayList();
		add(attrstr);
	}

	public AttributeList(String... attrs){
		attr = new ArrayList();
		for(int i=0; i < attrs.length; i++)
			add(attrs[i]);
	}

	public void add(String str){
		if(FunctionParameter.isFunction(str))
			attr.add(new FunctionParameter(str));
		else if(Attribute.isAttribute(str))
			attr.add(new Attribute(str));
		else
			attr.add(new String(str));
	}

	public void add(Attribute a){
		attr.add(a);
	}

	public void add(FunctionParameter f){
		attr.add(f);
	}
	
	public AttributeList copy(){
		AttributeList rval = new AttributeList();
		for(int i=0; i < attr.size(); i++){
			if(isFunction(i))
				rval.attr.add(getFunction(i).copy());
			else if(isAttribute(i))
				rval.attr.add(getAttribute(i).copy());
			else
				rval.attr.add(new String(getString(i)));
		}
		return rval;
	}

	public boolean equals(Object o){
		if(! (o instanceof AttributeList))
			return false;
		AttributeList a = (AttributeList)o;
		if(size() != a.size())
			return false;
		for(int i=0; i < size(); i++)
			if(! attr.get(i).equals(a.attr.get(i)))
				return false;
		return true;
	}


	public String getString(int i){
		return attr.get(i).toString();
	}

	public Attribute getAttribute(int index){
		return (Attribute)(attr.get(index));
	}

	public FunctionParameter getFunction(int index){
		return (FunctionParameter)(attr.get(index));
	}

	public boolean isAttribute(int index){
		return (attr.get(index) instanceof Attribute);
	}

	public boolean isFunction(int index){
		return (attr.get(index) instanceof FunctionParameter);
	}

	public boolean isAsterisk(){
		return size() == 1 && getString(0).equals("*");
	}

	public int size(){
		return attr.size();
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		for(int i=0; i < size(); i++){
			sb.append(getString(i));
			if(i + 1 < size())
				sb.append(",");
		}
		return sb.toString();
	}

	public int hashCode(){
		int rval = 0;
		for(int i=0; i < attr.size(); i++)
			rval += attr.get(i).hashCode();
		return rval;
	}

	public AttributeList concat(String newattr){
		AttributeList rval = copy();
		rval.add(newattr);
		return rval;
	}

	public AttributeList concat(AttributeList target){
		AttributeList rval = copy();
		for(int i=0; i < target.size(); i++)
			rval.attr.add(target.attr.get(i));
		return rval;
	}

	public boolean containsFunction(){
		for(int i=0; i < attr.size(); i++){
			if(isFunction(i))
				return true;
		}
		return false;
	}

	public FunctionParameter[] getFunctionParameters(){
		HashSet funcs = new HashSet();
		for(int i=0; i < attr.size(); i++){
			if(isFunction(i))
				funcs.add(getFunction(i));
		}
		FunctionParameter[] rval = new FunctionParameter[funcs.size()];
		rval = (FunctionParameter[])(funcs.toArray(rval));
		return rval;
	}


	/**
	 * This method returns new AttributeList by replacing all source names with a new source name.
	 */
	public AttributeList renameAllSourceNames(String to){
		AttributeList rval = copy();
		for(int i=0; i < rval.attr.size(); i++){
			if(rval.isAttribute(i)){
				Attribute a = rval.getAttribute(i);
				rval.attr.set(i, new Attribute(to, a.getColumnName()));
			}
			else if(rval.isFunction(i)){
				FunctionParameter f = rval.getFunction(i);
				AttributeList args = f.getArguments().renameAllSourceNames(to);
				rval.attr.set(i, new FunctionParameter(f.getFunctionName(), args));
			}
		}
		return rval;
	}


	/**
	 * This method returns new AttributeList by replacing an element with a new element.
	 */
	public AttributeList rename(String from, String to){
		AttributeList rval = new AttributeList();
		for(int i=0; i < size(); i++){
			String s = getString(i);
			if(from.equals(s))
				rval.add(to);
			else
				rval.add(s);
		}
		return rval;
	}


	/**
	 * This method returns new AttributeList by replaceing given source name with new source name.
	 */
	public AttributeList renameSourceName(String from, String to){
		AttributeList rval = copy();
		if(from == null || to == null || from.equals(to))
			return rval;
		for(int i=0; i < rval.attr.size(); i++){
			if(rval.isAttribute(i)){
				Attribute a = rval.getAttribute(i);
				if(a.getSourceName().equals(from))
					rval.attr.set(i, new Attribute(to, a.getColumnName()));
			}
			else if(rval.isFunction(i)){
				FunctionParameter f = rval.getFunction(i);
				AttributeList args = f.getArguments().renameSourceName(from, to);
				rval.attr.set(i, new FunctionParameter(f.getFunctionName(), args));
			}
		}
		return rval;
	}

	/**
	 * This method returns new AttributeList by replaceing given column name with new column name.
	 */
	public AttributeList renameColumnName(String from, String to){
		AttributeList rval = copy();
		if(from == null || to == null || from.equals(to))
			return rval;
		for(int i=0; i < rval.attr.size(); i++){
			if(rval.isAttribute(i)){
				Attribute a = rval.getAttribute(i);
				if(a.getColumnName().equals(from))
					rval.attr.set(i, new Attribute(a.getSourceName(), to));
			}
			else if(rval.isFunction(i)){
				FunctionParameter f = rval.getFunction(i);
				AttributeList args = f.getArguments().renameColumnName(from, to);
				rval.attr.set(i, new FunctionParameter(f.getFunctionName(), args));
			}
		}
		return rval;
	}

	public static AttributeList parse(String str) throws StreamSpinnerException {
		AttributeList rval = new AttributeList();
		String[] tokens = str.split("\\s*,\\s*");
		String buf = "";
		for(int i=0; tokens != null && i < tokens.length; i++){
			buf = buf + tokens[i];
			if(buf.indexOf("(") >= 0 && (! FunctionParameter.isFunction(buf))){
				buf = buf + ",";
				continue;
			}
			rval.add(buf);
			buf = "";
		}
		if(! buf.equals(""))
			throw new StreamSpinnerException("Cannot parse string : " + str);
		return rval;
	}
}
