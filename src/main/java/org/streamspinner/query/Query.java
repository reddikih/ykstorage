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

/*
 * Continuous Query
 */

public class Query implements Serializable {

	private static int counter = 0;

	private MasterSet master;
	private AttributeList select;
	private SourceSet from;
	private PredicateSet where;
	private AttributeList key;
	private RenameParameter rename;
	private TableManipulationParameter table;
	private Object owner;
	private Object qid;


	public Query(MasterSet m, AttributeList s, SourceSet f, PredicateSet w){
		master = m.copy();
		select = s.copy();
		from   = f.copy();
		where  = w.copy();
		key = null;
		rename = null;
		table = null;
		qid = new Integer(counter);
		owner = null;
		counter++;
	}

	public Query(MasterSet m, AttributeList s, SourceSet f, PredicateSet w, AttributeList k, RenameParameter r){
		this(m, s, f, w);
		if(k != null)
			key = k.copy();
		if(r != null)
			rename = r.copy();
	}

	public Query(MasterSet m, AttributeList s, SourceSet f, PredicateSet w, AttributeList k, RenameParameter r, TableManipulationParameter t){
		this(m, s, f, w, k, r);
		if(t != null)
			table = t.copy();
	}

	public Query(TableManipulationParameter t){
		this(new MasterSet(), new AttributeList(), new SourceSet(), new PredicateSet(), null, null, t);
	}

	public void setOwner(Object o){
		owner = o;
	}

	public void setID(Object id){
		qid = id;
	}

	public void setMasterClause(MasterSet m){
		master = m.copy();
	}

	public void setSelectClause(AttributeList s){
		select = s.copy();
	}

	public void setFromClause(SourceSet f){
		from = f.copy();
	}

	public void setWhereClause(PredicateSet w){
		where = w.copy();
	}

	public void setGroupByClause(AttributeList k){
		key = k.copy();
	}

	public void setRenameParameter(RenameParameter r){
		rename = r.copy();
	}

	public void setTableManipulationParameter(TableManipulationParameter t){
		table = t.copy();
	}

	public Object getID(){
		return qid;
	}

	public Object getOwner(){
		return owner;
	}

	public MasterSet getMasterClause(){
		return master;
	}

	public AttributeList getSelectClause(){
		return select;
	}

	public SourceSet getFromClause(){
		return from;
	}

	public PredicateSet getWhereClause(){
		return where;
	}

	public AttributeList getGroupByClause(){
		return key;
	}

	public RenameParameter getRenameParameter(){
		return rename;
	}

	public TableManipulationParameter getTableManipulationParameter(){
		return table;
	}

	public boolean hasConditions(){
		if(where != null && where.size() != 0)
			return true;
		else
			return false;
	}


	public String toString(){
		StringBuilder sb = new StringBuilder("");
		if(select != null && select.size() != 0){
			sb.append("SELECT  ");
			if(rename != null && (! rename.isTableRename()))
				sb.append(rename.rename(select).toString());
			else
				sb.append(select.toString());
			sb.append("  ");
		}
		if(from != null && from.size() != 0){
			sb.append("FROM  ");
			sb.append(from.toString());
		}
		if(where != null && where.size() > 0){
			sb.append("  WHERE  ");
			sb.append(where.toString());
		}
		if(key != null){
			sb.append("  GROUP BY  ");
			sb.append(key.toString());
		}
		if(table != null)
			sb = new StringBuilder(table.toString(sb.toString()));
		if(master != null && master.size() != 0)
			sb.insert(0, "MASTER  " + master.toString() + "  ");
		return sb.toString();
	}

}

