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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.Iterator;

public class LinkTable {

	private HashMap table;

	public LinkTable(){
		table = new HashMap();
	}


	public void add(Node key, Node value){
		LinkTableKey lkey = new LinkTableKey(key);
		if(table.containsKey(lkey)){
			ArrayList list = (ArrayList)(table.get(lkey));
			list.add(value);
		}
		else {
			ArrayList list = new ArrayList();
			list.add(value);
			table.put(lkey, list);
		}
	}

	public void addAll(Node key, Node[] value){
		LinkTableKey lkey = new LinkTableKey(key);
		if(table.containsKey(lkey)){
			ArrayList list = (ArrayList)(table.get(lkey));
			list.addAll(Arrays.asList(value));
		}
		else {
			ArrayList list = new ArrayList();
			list.addAll(Arrays.asList(value));
			table.put(lkey, list);
		}
	}

	public Node[] lookup(Node key){
		LinkTableKey lkey = new LinkTableKey(key);
		if(! table.containsKey(lkey))
			return null;
		ArrayList list = (ArrayList)(table.get(lkey));
		Node[] rvals = new Node[list.size()];
		rvals = (Node[])(list.toArray(rvals));
		return rvals;
	}

	public boolean containsKey(Node key){
		return lookup(key) != null;
	}

	public Node[] keys(){
		Set lkeys = table.keySet();
		Node[] rvals = new Node[lkeys.size()];
		Iterator it = lkeys.iterator();
		for(int i=0; i < rvals.length && it.hasNext(); i++){
			rvals[i] = ((LinkTableKey)(it.next())).get();
		}
		return rvals;
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		Node[] keys = keys();
		for(int i=0; i < keys.length; i++){
			sb.append("key:\n");
			sb.append("  " + keys[i].toString());
			sb.append("\n");
			sb.append("values:\n");
			Node[] values = lookup(keys[i]);
			for(int j=0; values != null && j < values.length; j++)
				sb.append("  " + values[j].toString() + "\n");
		}
		return sb.toString();
	}
			

	private class LinkTableKey {

		private Node node;

		public LinkTableKey(Node n){
			node = n;
		}

		public boolean equals(Object o){
			if(! (o instanceof LinkTableKey))
				return false;
			LinkTableKey target = (LinkTableKey)o;
			return node.equals(target.node);
		}

		public int hashCode(){
			return node.hashCode();
		}

		public Node get(){
			return node;
		}
	}

}
