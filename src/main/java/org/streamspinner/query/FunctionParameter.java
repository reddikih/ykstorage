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

import java.io.Serializable;
import java.util.Iterator;
import java.util.regex.*;

public class FunctionParameter implements Serializable {

	private String fname;
	private AttributeList args;

	private static Pattern p = Pattern.compile("^(\\w+)\\((.*)\\)$");

	private FunctionParameter(){
		;
	}

	public FunctionParameter(String str){
		Matcher m = p.matcher(str);
		if(! m.matches())
			throw new IllegalArgumentException("Not function");
		fname = m.group(1);
		if(m.group(2)!= null && (! m.group(2).equals("")) )
			args = new AttributeList(m.group(2).split(",\\s*"));
	}

	public FunctionParameter(String f, AttributeList a){
		fname = new String(f);
		args = a.copy();
	}

	public String getFunctionName(){
		return fname;
	}

	public AttributeList getArguments(){
		return args;
	}

	public FunctionParameter copy(){
		FunctionParameter rval = new FunctionParameter();
		rval.fname = new String(fname);
		rval.args = args.copy();
		return rval;
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(fname);
		sb.append("(");
		for(int i=0; i < args.size(); i++){
			sb.append(args.getString(i));
			if(i + 1 < args.size())
				sb.append(",");
		}
		sb.append(")");
		return sb.toString();
	}

	public boolean equals(Object o){
		if(! (o instanceof FunctionParameter))
			return false;
		FunctionParameter target = (FunctionParameter)o;
		if(! (fname.equalsIgnoreCase(target.getFunctionName())))
			return false;
		if(! args.equals(target.getArguments()))
			return false;
		return true;
	}

	public boolean isComputable(SourceSet sources){
		if(args == null)
			return false;

		for(int i=0; i < args.size(); i++){
			if(args.isAttribute(i)){
				Attribute a = args.getAttribute(i);
				if(sources.contains(a.getSourceName()))
					continue;
				else
					return false;
			}
			else if(args.isFunction(i)){
				FunctionParameter f = args.getFunction(i);
				if(f.isComputable(sources))
					continue;
				else
					return false;
			}
		}
		return true;
	}


	public int hashCode(){
		return toString().hashCode();
	}

	public static boolean isFunction(String str){
		Matcher m = p.matcher(str);
		return m.matches();
	}

}
