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
import java.io.Serializable;

public class MasterSet implements Serializable {

	private HashSet master;

	public MasterSet(){
		master = new HashSet();
	}

	public MasterSet(String m){
		this();
		master.add(m);
	}

	public MasterSet(String[] masters){
		this();
		for(int i=0; masters != null && i < masters.length; i++)
			master.add(new String(masters[i]));
	}

	public MasterSet copy(){
		MasterSet rval = new MasterSet();
		Iterator it = this.iterator();
		while(it.hasNext()){
			rval.add(it.next().toString());
		}
		return rval;
	}

	public void add(String m){
		master.add(m);
	}

	public boolean equals(Object o){
		if(! (o instanceof MasterSet))
			return false;
		MasterSet m = (MasterSet)o;
		if(this.size() != m.size())
			return false;
		Iterator it = this.iterator();
		while(it.hasNext()){
			if(! m.contains(it.next()))
				return false;
		}
		return true;
	}


	public boolean contains(Object o){
		return master.contains(o);
	}

	public int hashCode(){
		int rval = 0;
		Iterator it = iterator();
		while(it.hasNext())
			rval += it.next().hashCode();
		return rval;
	}


	public Iterator iterator(){
		return master.iterator();
	}

	public int size(){
		return master.size();
	}

	public String toString(){
		Iterator it = this.iterator();
		StringBuffer sb = new StringBuffer();

		while(it.hasNext()){
			sb.append(it.next().toString());
			if(it.hasNext())
				sb.append(",");
		}
		return sb.toString();
	}

	public MasterSet concat(MasterSet target){
		MasterSet rval = copy();
		rval.master.addAll(target.master);
		return rval;
	}
}
