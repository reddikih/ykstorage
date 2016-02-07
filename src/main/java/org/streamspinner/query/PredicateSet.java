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

import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.*;
import java.io.Serializable;

public class PredicateSet implements Serializable {

	private HashSet pred;
	private static Pattern pcond = Pattern.compile("(((\\w+\\.)\\w+)|('.*')|(\\d+\\.\\d+)|(\\d+)|(\\w+\\(.*\\)))\\s*(>|<|=|>=|<=|!=)\\s*(((\\w+\\.)\\w+)|('.*')|(\\d+\\.\\d+)|(\\d+)|(\\w+\\(.*\\)))");

	public PredicateSet(String condstr){
		this();
		Matcher m = pcond.matcher(condstr);
		while(m.find())
			add(new Predicate(m.group(1), m.group(8), m.group(9)));
	}

	public PredicateSet(){
		pred = new HashSet();
	}

	public PredicateSet(String[][] p){
		pred = new HashSet();
		for(int i=0; i < p.length; i++)
			pred.add(new Predicate(p[i][0], p[i][1], p[i][2]));
	}

	public PredicateSet(Predicate[] p){
		this();
		for(int i=0; p != null && i < p.length; i++)
			pred.add(p[i]);
	}

	public PredicateSet(Predicate p){
		this();
		pred.add(p);
	}

	public PredicateSet copy(){
		PredicateSet rval = new PredicateSet();
		Iterator it = iterator();
		while(it.hasNext()){
			rval.add((Predicate)(it.next()));
		}
		return rval;
	}

	public void add(String l, String op, String r){
		pred.add(new Predicate(l, op, r));
	}

	public void add(Predicate p){
		pred.add(p);
	}

	public boolean equals(Object o){
		if(! (o instanceof PredicateSet))
			return false;
		PredicateSet p = (PredicateSet)o;
		if(size() != p.size())
			return false;
		Iterator it = iterator();
		while(it.hasNext()){
			if(! p.contains(it.next()))
				return false;
		}
		return true;
	}


	public boolean contains(Object o){
		return pred.contains(o);
	}

	public Iterator iterator(){
		return pred.iterator();
	}

	public int size(){
		return pred.size();
	}

	public PredicateSet concat(PredicateSet target){
		PredicateSet rval = copy();
		Iterator pit = target.iterator();
		while(pit.hasNext()){
			rval.add((Predicate)(pit.next()));
		}
		return rval;
	}

	public String toString(){
		StringBuffer sb = new StringBuffer("");

		Iterator it = iterator();
		while(it.hasNext()){
			sb.append(it.next().toString());
			if(it.hasNext())
				sb.append("  AND  ");
		}
		return sb.toString();
	}

	public PredicateSet diff(PredicateSet target){
		PredicateSet rval = copy();
		Iterator pit = target.iterator();
		while(pit.hasNext()){
			Object p = pit.next();
			if(rval.pred.contains(p))
				rval.pred.remove(p);
		}
		return rval;
	}


	public int hashCode(){
		Iterator it = iterator();
		int rval = 0;
		while(it.hasNext()){
			rval += it.next().hashCode();
		}
		return rval;
	}

	public PredicateSet getJoinConditionsOf(SourceSet sources){
		PredicateSet rval = new PredicateSet();
		Iterator pit = iterator();
		while(pit.hasNext()){
			Predicate p = (Predicate)(pit.next());
			if(p.isJoinConditionOf(sources))
				rval.add(p);
		}
		return rval;
	}

	public PredicateSet getSelectionConditionsOf(SourceSet sources){
		PredicateSet rval = new PredicateSet();
		Iterator pit = iterator();
		while(pit.hasNext()){
			Predicate p = (Predicate)(pit.next());
			if(p.isSelectionConditionOf(sources))
				rval.add(p);
		}
		return rval;
	}

	public boolean containsFunction(){
		Iterator pit = iterator();
		while(pit.hasNext()){
			Predicate p = (Predicate)(pit.next());
			if(p.containsFunction())
				return true;
		}
		return false;
	}

	public FunctionParameter[] extractFunctions(){
		HashSet functions = new HashSet();
		Iterator pit = iterator();
		while(pit.hasNext()){
			Predicate p = (Predicate)(pit.next());
			if(p.containsFunction()){
				FunctionParameter[] f = p.extractFunctions();
				for(int i=0; f != null && i < f.length; i++)
					functions.add(f[i]);
			}
		}
		FunctionParameter[] rval = new FunctionParameter[functions.size()];
		rval = (FunctionParameter[])(functions.toArray(rval));
		return rval;
	}

}
