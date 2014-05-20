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

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.DataTypes;
import org.streamspinner.query.FunctionParameter;
import org.streamspinner.query.AttributeList;
import org.streamspinner.engine.Schema;
import java.lang.reflect.Method;
import javax.xml.parsers.*;
import java.io.*;
import org.xml.sax.*;
import org.w3c.dom.*;
import java.util.*;

/**
 * This class expresses function invoked by StreamSpinner.
 * <p>
 * At initialization step, StreamSpinner loads definition files from a directory.
 * (Default is "./conf/functions/*.xml")
 * </p>
 * <p>
 * The following is an example of definition files:
 * <pre>
 * &lt;?xml version="1.0" encoding="iso-8859-1" ?&gt;
 * &lt;function name="abs" aggregate="false"&gt;
 *   &lt;parameter name="argtypes" value="Double" /&gt;
 *   &lt;parameter name="returntype" value="Double" /&gt;
 *   &lt;parameter name="owner" value="java.lang.Math" /&gt;
 *   &lt;parameter name="method" value="abs" /&gt;
 * &lt;/function&gt;
 * </pre>
 * This example adds a new function "abs(Double)" to StreamSpinner.
 * When the method is used in a query, StreamSpinner invokes java.lang.Math.abs(double).
 * </p>
 * <p>
 * In the current implementation, callable methods are limited only static methods.
 * </p>
 */

public class Function {

	private static Hashtable<FKey, Function> functions = new Hashtable<FKey, Function>();
	private static Hashtable<String, Boolean> aggregatechecker = new Hashtable<String, Boolean>();

	private static class FKey {
		private String functionname;
		private List<String> argtypes;
		private FKey(String n, String... args){
			functionname = n;
			argtypes = Arrays.asList(args);
		}
		public int hashCode(){
			int rval = functionname.hashCode();
			for(String arg : argtypes)
				rval += arg.hashCode();
			return rval;
		}
		public boolean equals(Object obj){
			if(! (obj instanceof FKey))
				return false;
			FKey fk = (FKey)(obj);
			return functionname.equals(fk.functionname) && argtypes.equals(fk.argtypes);
		}
		public String toString(){
			StringBuilder sb = new StringBuilder(functionname);
			sb.append("(");
			for(int i=0; i < argtypes.size(); i++){
				sb.append(argtypes.get(i));
				if(i < argtypes.size() -1)
					sb.append(",");
			}
			sb.append(")");
			return sb.toString();
		}
	}

	private static class SourceFileFilter implements FilenameFilter {
		public boolean accept(File dir, String name){
			return name.endsWith(".xml") || name.endsWith(".XML");
		}
	}

	/**
	 * This method tries to load configuration files for functions from specified directory.
	 */
	public static void loadFunctions(String dirstr) throws StreamSpinnerException {
		try {
			File dir = new File(dirstr);
			if(! dir.isDirectory())
				throw new IOException("Parameter is not a directory.");
			File[] sourcefiles = dir.listFiles(new SourceFileFilter());
			for(File file : sourcefiles){
				Function f = Function.parse(file);
				String name = f.getFunctionName();
				if(functions.containsKey(f.key))
					throw new StreamSpinnerException("Conflicting definitions are detected: " + f.key.toString());
				if(aggregatechecker.containsKey(name) && (! aggregatechecker.get(name).equals(f.isAggregateFunction())) )
					throw new StreamSpinnerException("CONFLICT! Two types of functions have the same name: aggregate function \"" + name + "\" and normal function \"" + name + "\"");
				functions.put(f.key, f);
				aggregatechecker.put(name, f.isAggregateFunction());
			}
		} catch(Exception e){
			throw new StreamSpinnerException(e);
		}
	}

	private static Function parse(File f) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document d = db.parse(f);

		Element fnode = d.getDocumentElement();
		NamedNodeMap fnamemap = fnode.getAttributes();
		String fname = fnamemap.getNamedItem("name").getNodeValue();
		String aggregate = fnamemap.getNamedItem("aggregate").getNodeValue();

		HashMap<String,String> parameters = new HashMap<String,String>();

		NodeList plist = fnode.getElementsByTagName("parameter");
		for(int i=0; i < plist.getLength(); i++){
			Node pnode = plist.item(i);
			NamedNodeMap pmap = pnode.getAttributes();
			String pname = pmap.getNamedItem("name").getNodeValue();
			String pvalue = pmap.getNamedItem("value").getNodeValue();
			parameters.put(pname, pvalue);
		}

		String[] argtypestr = parameters.get("argtypes").split("\\s*,\\s*");
		String returntypestr = parameters.get("returntype");
		String ownerstr = parameters.get("owner");
		String methodstr = parameters.get("method");

		return new Function(fname, argtypestr, returntypestr, ownerstr, methodstr, aggregate);
	}


	public static Function getInstance(String fname, String... argtypes) throws StreamSpinnerException {
		FKey key = new FKey(fname, argtypes);
		if(functions.containsKey(key))
			return functions.get(key);
		else
			throw new StreamSpinnerException("function " + key.toString() + " is not found");
	}

	public static Function getInstance(FunctionParameter fp, Schema s) throws StreamSpinnerException {
		String fname = fp.getFunctionName();
		AttributeList attr = fp.getArguments();
		String[] types = new String[attr.size()];
		for(int i=0; i < attr.size(); i++){
			try {
				types[i] = s.getType(attr.getString(i));
			} catch(IllegalArgumentException iae){
				throw new StreamSpinnerException(iae);
			}
		}
		return getInstance(fname, types);
	}

	/**
	 * This method returns true if the function is already defined.
	 */
	public static boolean exists(String name, String... argtypes){
		FKey key = new FKey(name, argtypes);
		return functions.containsKey(key);
	}

	/**
	 * This method returns true if the specified function name is reserved for aggregate functions.
	 * When the specified name is not used for any functions, this method always returns false.
	 */
	public static boolean isAggregateFunction(String name){
		if(aggregatechecker.containsKey(name))
			return aggregatechecker.get(name);
		else
			return false;
	}


	private String name;
	private Class owner;
	private Class[] argtypes;
	private Method method;
	private String returntype;
	private FKey key;
	private boolean aggregate;

	/**
	 * This constructor create new Function object.
	 *
	 * @param fname Name of function used in StreamSpinner
	 * @param argtypestr Array of types. Each type must be a string defined by {@link org.streamspinner.DataTypes}.
	 * @param returntypestr Type of return value. It must be a string defined by {@link org.streamspinner.DataTypes}.
	 * @param ownerstr The name of the class having the method.
	 * @param methodstr The name of the method invoked by StreamSpinner.
	 * @param aggregatestr This parameter specifies whether the function is an aggregate function or not.
	 */
	public Function(String fname, String[] argtypestr, String returntypestr, String ownerstr, String methodstr, String aggregatestr) throws StreamSpinnerException {
		try {
			name = fname;
			argtypes = new Class[argtypestr.length];
			for(int i=0; i < argtypestr.length; i++)
				argtypes[i] = DataTypes.getClass(argtypestr[i]);
			returntype = returntypestr;
			owner = Class.forName(ownerstr);
			method = owner.getDeclaredMethod(methodstr, argtypes);
			key = new FKey(fname, argtypestr);
			aggregate = (new Boolean(aggregatestr)).booleanValue();
		} catch(Exception e){
			throw new StreamSpinnerException(e);
		}
	}

	public String getFunctionName(){
		return name;
	}

	public Class getMethodOwner(){
		return owner;
	}

	public Method getMethod(){
		return method;
	}

	public String getReturnType(){
		return returntype;
	}

	public Class[] getArgumentTypes(){
		return argtypes;
	}

	public int getArgumentLength(){
		return argtypes.length;
	}

	public boolean isAggregateFunction(){
		return aggregate;
	}

	public boolean isNormalFunction(){
		return ! aggregate;
	}

	public Object invoke(Object[] args) throws StreamSpinnerException {
		try {
			return method.invoke(null, args);
		} catch(Exception e){
			return null;
		}
	}

}
