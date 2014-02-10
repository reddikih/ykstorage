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

public class ORNode implements Serializable, Node {

	private AttributeList attrs;
	private SourceSet sources;
	private PredicateSet conds;
	private AttributeList groupkey;
	private RenameParameter rename;
	private TableManipulationParameter table;

	public ORNode(AttributeList a, SourceSet s, PredicateSet p){
		if(a == null || s == null || p == null)
			throw new NullPointerException("Arguments must not be null");
		attrs = a.copy();
		sources = s.copy();
		conds = p.copy();
		groupkey = null;
		rename = null;
		table = null;
	}

	public ORNode(AttributeList a, SourceSet s, PredicateSet p, AttributeList k){
		this(a, s, p);
		if(k != null)
			groupkey = k.copy();
	}

	public ORNode(AttributeList a, SourceSet s, PredicateSet p, AttributeList k, RenameParameter r){
		this(a, s, p, k);
		if(r != null)
			rename = r.copy();
	}

	public ORNode(AttributeList a, SourceSet s, PredicateSet p, AttributeList k, RenameParameter r, TableManipulationParameter t){
		this(a, s, p, k, r);
		if(t != null)
			table = t.copy();
	}

	public ORNode(TableManipulationParameter t){
		this(new AttributeList("*"), new SourceSet(), new PredicateSet());
		if(t != null)
			table = t.copy();
	}


	private ORNode(){
		;
	}


	public ORNode copy(){
		ORNode on = new ORNode();
		on.attrs = attrs.copy();
		on.sources = sources.copy();
		on.conds = conds.copy();
		if(groupkey != null)
			on.groupkey = groupkey.copy();
		if(rename != null)
			on.rename = rename.copy();
		if(table != null)
			on.table = table.copy();

		return on;
	}

	public boolean isBase(){
		if(sources.size() == 1 && conds.size() == 0 && attrs.isAsterisk() && groupkey == null && rename == null && table == null)
			return true;
		else 
			return false;
	}

	public boolean equals(Object target){
		if(! (target instanceof ORNode))
			return false;
		ORNode on = (ORNode)target;
		if(! attrs.equals(on.attrs))
			return false;
		if(! sources.equals(on.sources))
			return false;
		if(! conds.equals(on.conds))
			return false;
		if((groupkey == null && on.groupkey != null) || (groupkey != null && (! groupkey.equals(on.groupkey))) )
			return false;
		if((rename == null && on.rename != null) || (rename != null && (! rename.equals(on.rename))) )
			return false;
		if((table == null && on.table != null) || (table != null && (! table.equals(on.table))) )
			return false;
		return true;
	}


	public int hashCode(){
		int rval = 0;
		rval += attrs.hashCode();
		rval += sources.hashCode();
		rval += conds.hashCode();
		if(groupkey != null)
			rval += groupkey.hashCode();
		if(rename != null)
			rval += rename.hashCode();
		if(table != null)
			rval += table.hashCode();
		return rval;
	}

	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		if(rename == null || rename.isTableRename())
			sb.append(attrs.toString());
		else
			sb.append(rename.toString(attrs));
		sb.append(" FROM ");
		sb.append(sources.toString());
		if(conds.size() > 0){
			sb.append(" WHERE ");
			sb.append(conds.toString());
		}
		if(groupkey != null){
			sb.append(" GROUP BY ");
			sb.append(groupkey.toString());
		}
		if(rename != null && rename.isTableRename()){
			sb.insert(0, "( ");
			sb.append(" ) AS ");
			sb.append(rename.getNewTableName());
		}
		if(table != null)
			sb = new StringBuilder(table.toString(sb.toString()));
		return sb.toString();
	}

	public SourceSet getSources(){
		return sources;
	}

	public PredicateSet getConditions(){
		return conds;
	}

	public AttributeList getAttributeList(){
		return attrs;
	}

	public AttributeList getGroupingKeys(){
		return groupkey;
	}

	public RenameParameter getRenameParameter(){
		return rename;
	}

	public TableManipulationParameter getTableManipulationParameter(){
		return table;
	}

	public boolean isANDNode(){
		return false;
	}

	public boolean isORNode(){
		return true;
	}

}

