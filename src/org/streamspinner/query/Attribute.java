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
import java.util.regex.*;

public class Attribute implements Serializable {

	private static Pattern p = Pattern.compile("^([a-zA-Z]\\w*)\\.([a-zA-Z]\\w*)$");

	private String sourcename;
	private String columnname;

	private Attribute(){
		;
	}

	public Attribute(String str){
		Matcher m = p.matcher(str);
		if(! m.matches())
			throw new IllegalArgumentException("Not Attribute");
		sourcename = m.group(1);
		columnname = m.group(2);
	}

	public Attribute(String sname, String cname){
		sourcename = sname;
		columnname = cname;
	}

	public String getSourceName(){
		return sourcename;
	}

	public String getColumnName(){
		return columnname;
	}

	public String toString(){
		return sourcename + "." + columnname;
	}

	public Attribute copy(){
		Attribute rval = new Attribute();
		rval.sourcename = getSourceName();
		rval.columnname = getColumnName();
		return rval;
	}


	public boolean equals(Object o){
		if(! (o instanceof Attribute))
			return false;
		Attribute a = (Attribute)o;
		return sourcename.equals(a.sourcename) && columnname.equals(a.columnname);
	}

	public int hashCode(){
		return toString().hashCode();
	}

	public static boolean isAttribute(String str){
		Matcher m = p.matcher(str);
		return m.matches();
	}

}
