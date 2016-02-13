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
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.Serializable;

public class NestedSourceSet extends SourceSet implements Serializable {

	private HashSet<ORNode> ornode;
	private HashMap<String,Long> windowsizes;
	private HashMap<String,Long> origins;

	public NestedSourceSet(){
		ornode = new HashSet<ORNode>();
		windowsizes = new HashMap<String,Long>();
		origins = new HashMap<String,Long>();
	}

	public NestedSourceSet(SourceSet ss){
		this();
		for(Iterator<String> it = ss.iterator(); it.hasNext(); ){
			String s = it.next();
			add(s, ss.getWindowsize(s), ss.getWindowOrigin(s));
		}
	}

	public NestedSourceSet(String source, long window){
		this();
		add(source, window);
	}

	public NestedSourceSet(String source, long window, long origin){
		this();
		add(source, window, origin);
	}

	public NestedSourceSet(String[] sources, long[] window){
		this();
		for(int i=0; i < sources.length; i++)
			add(sources[i], window[i]);
	}

	public NestedSourceSet(String[] sources, long[] window, long[] origin){
		this();
		for(int i=0; i < sources.length; i++)
			add(sources[i], window[i], origin[i]);
	}

	public NestedSourceSet(ORNode o){
		this();
		add(o);
	}

	public NestedSourceSet(ORNode[] o){
		this();
		for(int i=0; i < o.length; i++)
			add(o[i]);
	}

	public NestedSourceSet(ORNode[] o, String[] sources, long[] window){
		this();
		for(int i=0; o != null && i < o.length; i++)
			add(o[i]);
		for(int i=0; i < sources.length; i++)
			add(sources[i], window[i]);
	}

	public NestedSourceSet(ORNode[] o, String[] sources, long[] window, long[] origin){
		this();
		for(int i=0; o != null && i < o.length; i++)
			add(o[i]);
		for(int i=0; i < sources.length; i++)
			add(sources[i], window[i], origin[i]);
	}

	public void add(ORNode o){
		ornode.add(o);
	}

	public void add(String s, long win){
		add(s, win, 0);
	}

	public void add(String s, long win, long origin){
		if((! contains(s)) || windowsizes.containsKey(s)){
			windowsizes.put(s, new Long(win));
			origins.put(s, new Long(origin));
		}
		else {
			for(ORNode o : ornode ){
				SourceSet ss = o.getSources();
				if(ss.contains(s)){
					ss.add(s, win, origin);
					break;
				}
			}
		}
	}

	public SourceSet copy(){
		NestedSourceSet rval = new NestedSourceSet();
		rval.ornode = new HashSet<ORNode>();
		for(ORNode on : ornode)
			rval.ornode.add(on.copy());
		rval.windowsizes = new HashMap<String,Long>(windowsizes);
		rval.origins = new HashMap<String,Long>(origins);
		return rval;
	}

	public boolean contains(Object key){
		if(windowsizes.containsKey(key))
			return true;
		for(ORNode o : ornode){
			SourceSet s = o.getSources();
			if(s.contains(key))
				return true;
		}
		return false;
	}

	public long getWindowsize(Object key){
		if(windowsizes.containsKey(key))
			return windowsizes.get(key).longValue();
		for(ORNode o : ornode){
			SourceSet s = o.getSources();
			if(s.contains(key))
				return s.getWindowsize(key);
		}
		return -1;
	}

	public long getWindowOrigin(Object key){
		if(origins.containsKey(key))
			return origins.get(key).longValue();
		for(ORNode o : ornode){
			SourceSet s = o.getSources();
			if(s.contains(key))
				return s.getWindowOrigin(key);
		}
		return 0;
	}

	public boolean equals(Object obj){
		NestedSourceSet target;
		if(! (obj instanceof NestedSourceSet)){
			if( ! (obj instanceof SourceSet))
				return false;
			else
				target = new NestedSourceSet((SourceSet)obj);
		}
		else
			target = (NestedSourceSet)obj;
		if(ornode.size() != target.ornode.size())
			return false;
		for(ORNode o : ornode){
			if(! target.ornode.contains(o))
				return false;
		}
		if(windowsizes.size() != target.windowsizes.size())
			return false;
		for(String s : windowsizes.keySet()){
			if(! target.windowsizes.containsKey(s))
				return false;
			if(! windowsizes.get(s).equals(target.windowsizes.get(s)))
				return false;
		}
		for(String s : origins.keySet()){
			if(! target.origins.containsKey(s))
				return false;
			if(! origins.get(s).equals(target.origins.get(s)))
				return false;
		}
		return true;
	}

	public Iterator<String> iterator(){
		ArrayList<String> slist = new ArrayList<String>();
		for(ORNode o : ornode){
			SourceSet ss = o.getSources();
			for(Iterator<String> sit = ss.iterator(); sit.hasNext(); )
				slist.add(sit.next());
		}
		for(String s : windowsizes.keySet())
			slist.add(s);
		return slist.iterator();
	}

	public int size(){
		int counter = 0;
		for(ORNode o : ornode)
			counter += o.getSources().size();
		counter += windowsizes.size();
		return counter;
	}

	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(Iterator<ORNode> oit = ornode.iterator(); oit.hasNext(); ){
			ORNode o = oit.next();
			if(o.getRenameParameter() != null && o.getRenameParameter().isTableRename())
				sb.append(o.toString());
			else {
				sb.append("( ");
				sb.append(o.toString());
				sb.append(" )");
			}
			if(oit.hasNext() || windowsizes.size() != 0)
				sb.append(",");
		}
		for(Iterator<String> sit = windowsizes.keySet().iterator(); sit.hasNext(); ){
			String s = sit.next();
			sb.append(s);
			long win = windowsizes.get(s).longValue();
			long origin = origins.get(s).longValue();
			if(win != Long.MAX_VALUE || origin != 0){
				sb.append("[" + win);
				if(origin > 0)
					sb.append(","+origin);
				else if(origin < 0)
					sb.append(",now"+origin);
				sb.append("]");
			}
			if(sit.hasNext())
				sb.append(",");
		}
		return sb.toString();
	}

	public int hashCode(){
		int rval = 0;
		for(ORNode o : ornode)
			rval += o.hashCode();
		for(String s : windowsizes.keySet())
			rval += s.hashCode();
		return rval;
	}

	public SourceSet concat(SourceSet target){
		NestedSourceSet rval = (NestedSourceSet)(copy());
		if(target instanceof NestedSourceSet){
			NestedSourceSet ntarget = (NestedSourceSet)target;
			for(ORNode o : ntarget.ornode)
				if(! rval.ornode.contains(o))
					rval.ornode.add(o);
		}
		for(Iterator<String> sit = target.iterator(); sit.hasNext(); ){
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

	public ORNode[] getORNodes(){
		return (ORNode[])(ornode.toArray(new ORNode[0]));
	}

	public int sizeOfORNodes(){
		return ornode.size();
	}
}
