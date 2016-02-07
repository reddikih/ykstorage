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

public class RenameParameter implements Serializable {

	private AttributeList attrs;

	public RenameParameter(AttributeList a){
		attrs = a;
	}

	public RenameParameter(String... args){
		attrs = new AttributeList(args);
	}

	public boolean equals(Object o){
		if(! (o instanceof RenameParameter))
			return false;
		RenameParameter target = (RenameParameter)(o);
		return attrs.equals(target.attrs);
	}

	public int hashCode(){
		return attrs.hashCode();
	}

	public AttributeList getAttributeList(){
		return attrs;
	}

	public boolean isTableRename(){
		return attrs.size() == 1 && attrs.getString(0).endsWith(".*");
	}

	public String getNewTableName(){
		if(! isTableRename())
			return "";
		String pattern = attrs.getString(0);
		String tablename = pattern.substring(0, pattern.indexOf(".*"));
		return tablename;
	}

	public AttributeList rename(AttributeList base){
		if(isTableRename()){
			return base.renameAllSourceNames(getNewTableName());
		}
		else if(attrs.size() != base.size())
			throw new IllegalArgumentException("size of attributes are invalid: expected value is "+attrs.size()+", but given value is " + base.size() );
		else
			return attrs.copy();
	}

	public String toString(){
		return attrs.toString();
	}

	public String toString(AttributeList base){
		AttributeList newattrs;
		if(isTableRename())
			newattrs = rename(base);
		else
			newattrs = attrs;
		if(base.size() == 1 && base.getString(0).equals("*"))
			return base.toString();
		if(newattrs.size() != base.size())
			throw new IllegalArgumentException("size of attributes are invalid: expected value is "+newattrs.size()+", but given value is " + base.size() );

		StringBuilder sb = new StringBuilder();
		for(int i=0; i < base.size() && i < newattrs.size(); i++){
			String b = base.getString(i);
			String a = newattrs.getString(i);
			sb.append(b);
			if(! a.equals(b)){
				sb.append(" AS ");
				sb.append(a);
			}
			if(i < base.size() -1 && i < newattrs.size() -1)
				sb.append(",");
		}
		return sb.toString();
	}

	public RenameParameter copy() {
		return new RenameParameter(attrs.copy());
	}

}
