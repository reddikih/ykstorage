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

import java.util.HashMap;
import java.util.Iterator;
import java.io.Serializable;

public class SourceSet implements Serializable {

	private HashMap<String, Long> windowsizes;
	private HashMap<String, Long> origins;

	public SourceSet(){
		windowsizes = new HashMap<String, Long>();
		origins = new HashMap<String, Long>();
	}

	public SourceSet(String source){
		this();
		add(source);
	}

	public SourceSet(String source, long window){
		this();
		add(source, window);
	}

	public SourceSet(String[] sources, long[] wins){
		this();
		for(int i=0; i < sources.length; i++)
			add(sources[i], wins[i]);
	}

	public SourceSet(String[] sources, long[] wins, long[] orig){
		this();
		for(int i=0; i < sources.length; i++)
			add(sources[i], wins[i], orig[i]);
	}

	public void add(String s){
		add(s, Long.MAX_VALUE, 0);
	}

	public void add(String s, long win){
		add(s, win, 0);
	}

	public void add(String s, long win, long origin){
		windowsizes.put(s, win);
		origins.put(s, origin);
	}

	public SourceSet copy(){
		SourceSet rval = new SourceSet();
		rval.windowsizes = new HashMap<String, Long>(windowsizes);
		rval.origins = new HashMap<String, Long>(origins);
		return rval;
	}

	public boolean contains(Object key){
		return windowsizes.containsKey(key) && origins.containsKey(key);
	}

	public long getWindowsize(Object key){
		if(! windowsizes.containsKey(key))
			return -1;
		return windowsizes.get(key).longValue();
	}

	public long getWindowOrigin(Object key){
		if(! origins.containsKey(key))
			return 0;
		return origins.get(key).longValue();
	}

	public boolean equals(Object o){
		if(! (o instanceof SourceSet))
			return false;
		SourceSet target = (SourceSet)o;
		if(size() != target.size())
			return false;
		Iterator<String> it = iterator();
		while(it.hasNext()){
			String s = it.next();
			if(! target.contains(s))
				return false;
			if(getWindowsize(s) != target.getWindowsize(s))
				return false;
			if(getWindowOrigin(s) != target.getWindowOrigin(s))
				return false;
		}
		return true;
	}


	public Iterator<String> iterator(){
		return windowsizes.keySet().iterator();
	}

	public int size(){
		return windowsizes.size();
	}

	public String toString(){
		StringBuilder sb = new StringBuilder();

		Iterator<String> it = iterator();
		while(it.hasNext()){
			String s = it.next();
			sb.append(s);
			long win = getWindowsize(s);
			long origin = getWindowOrigin(s);
			if(win != Long.MAX_VALUE || origin != 0){
				sb.append("[" + win);
				if(origin > 0)
					sb.append("," + origin);
				else if(origin < 0)
					sb.append(",now" + origin);
				sb.append("]");
			}
			if(it.hasNext())
				sb.append(",");
		}
		return sb.toString();
	}

	public int hashCode(){
		Iterator<String> it = iterator();
		int rval = 0;
		while(it.hasNext())
			rval += it.next().hashCode();
		return rval;
	}

	public SourceSet concat(SourceSet target){
		SourceSet rval = copy();
		Iterator<String> sit = target.iterator();
		while(sit.hasNext()){
			String s = sit.next();
			long w = target.getWindowsize(s);
			long o = target.getWindowOrigin(s);
			if(! contains(s))
				rval.add(s, w, o);
			else {
				long mywin = getWindowsize(s);
				long myorigin = getWindowOrigin(s);
				rval.add(s, Math.max(mywin, w) + Math.abs(myorigin - o), Math.max(myorigin, o));
			}
		}
		return rval;
	}

}
