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

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.Operators;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;

public class OperatorGroup implements Operators, Comparable, Serializable {

	private String type;
	private MasterSet masters;
	private HashSet<ANDNode> nodes;
	private HashSet<ORNode> inputs;
	private SourceSet sources;
	private boolean cacheproducer;
	private boolean cacheconsumer;
	private boolean executable;
	private int depth;
	private long lastexecutiontime;

	private void init(ANDNode base){
		type = base.getType();
		masters = base.getMasterSet();
		nodes = new HashSet<ANDNode>();
		nodes.add(base);
		inputs = new HashSet<ORNode>();
		inputs.addAll(Arrays.asList(base.getInputORNodes()));
		sources = base.getSourceSet();
		cacheproducer = false;
		cacheconsumer = false;
		executable = true;
		depth = -1;
		lastexecutiontime = 0;
	}

	private void init(OperatorGroup base){
		type = base.type;
		masters = base.masters;
		nodes = new HashSet<ANDNode>(base.nodes);
		inputs = new HashSet<ORNode>(base.inputs);
		sources = base.getSourceSet();
		cacheproducer = false;
		cacheconsumer = false;
		executable = true;
		depth = base.getDepth();
		lastexecutiontime = base.lastexecutiontime;
	}


	public OperatorGroup(ANDNode base){
		init(base);
	}

	public OperatorGroup(ANDNode n1, ANDNode n2) throws StreamSpinnerException {
		init(n1);
		add(n2);
	}

	public OperatorGroup(OperatorGroup og, ANDNode node) throws StreamSpinnerException {
		init(og);
		add(node);
	}

	public OperatorGroup(OperatorGroup og1, OperatorGroup og2) throws StreamSpinnerException {
		init(og1);
		add(og2);
	}

	public void add(ANDNode node) throws StreamSpinnerException {
		if(! type.equals(node.getType()))
			throw new StreamSpinnerException("Different node types are given");
		if(! masters.equals(node.getMasterSet())){
			if(type.equals(Operators.ROOT))
				throw new StreamSpinnerException("Nodes are triggered by different masters");
			else
				masters = masters.concat(node.getMasterSet());
		}

		for(ORNode i : node.getInputORNodes()){
			if(! inputs.contains(i))
				throw new StreamSpinnerException("Nodes have different inputs");
		}

		if(! nodes.contains(node))
			nodes.add(node);
		else {
			for(ANDNode a : nodes){
				if(a.equals(node)){
					for(Object qid : node.getQueryIDSet())
						a.addQueryID(qid);
					break;
				}
			}
		}
		sources = sources.concat(node.getSourceSet());
	}

	public void add(OperatorGroup og) throws StreamSpinnerException {
		if(! type.equals(og.type))
			throw new StreamSpinnerException("Different node types are given");
		if(! masters.equals(og.masters)){
			if(type.equals(Operators.ROOT))
				throw new StreamSpinnerException("Nodes are triggered by different masters");
			else
				masters = masters.concat(og.getMasterSet());
		}
		for(ORNode i : og.inputs)
			if(! inputs.contains(i))
				throw new StreamSpinnerException("Nodes have different inputs");
		for(ANDNode a : og.nodes)
			add(a);
		sources = sources.concat(og.sources);
		setDepth(Math.min(getDepth(), og.getDepth()));
	}

	public int compareTo(Object o){
		OperatorGroup target = (OperatorGroup)o;
		if(getDepth() > target.getDepth())
			return 1;
		else if(getDepth() < target.getDepth())
			return -1;
		else 
			return 0;
	}

	public boolean equals(Object obj){
		if(! (obj instanceof OperatorGroup))
			return false;
		OperatorGroup target = (OperatorGroup)obj;
		if(! getType().equals(target.getType()))
			return false;
		if(! getMasterSet().equals(target.getMasterSet()))
			return false;
		if(! getSourceSet().equals(target.getSourceSet()))
			return false;
		if(size() == 0 || target.size() == 0)
			return false;

		ANDNode a1 = getANDNodes()[0];
		ANDNode a2 = target.getANDNodes()[0];

		if(getType().equals(Operators.ROOT))
			return a1.equals(a2);
		else {
			ORNode o1 = a1.getOutputORNode();
			ORNode o2 = a2.getOutputORNode();
			return o1.equals(o2);
		}
	}

	public boolean isCommonOperator(OperatorGroup og, boolean ignoreMaster){
		if(! type.equals(og.type))
			return false;
		if(size() == 0 || og.size() == 0)
			return false;

		if(type.equals(Operators.ROOT)){
			if(! masters.equals(og.masters))
				return false;
			return inputs.equals(og.inputs);
		}
		else {
			if(ignoreMaster==false && (! masters.equals(og.masters)))
				return false;
			ORNode o1 = getANDNodes()[0].getOutputORNode();
			ORNode o2 = og.getANDNodes()[0].getOutputORNode();
			return inputs.equals(og.inputs) && o1.equals(o2);
		}
	}


	public ANDNode[] getANDNodes(){
		return (ANDNode[])(nodes.toArray(new ANDNode[0]));
	}

	public ORNode[] getInputORNodes(){
		return (ORNode[])(inputs.toArray(new ORNode[0]));
	}

	public int getDepth(){
		return depth;
	}

	public long getLastExecutionTime(){
		return lastexecutiontime;
	}

	public MasterSet getMasterSet(){
		return masters;
	}

	public String getType(){
		return type;
	}

	public SourceSet getSourceSet(){
		return sources;
	}

	public int hashCode(){
		return masters.hashCode();
	}

	public boolean isExecutable(){
		return executable;
	}

	public void remove(ANDNode node){
		nodes.remove(node);
	}

	public void remove(Query q){
		ArrayList<ANDNode> removelist = new ArrayList<ANDNode>();
		for(ANDNode a : nodes){
			if(a.getQueryIDSet().contains(q.getID()))
				a.removeQueryID(q.getID());
			if(a.getSharedCount() <= 0)
				removelist.add(a);
		}
		nodes.removeAll(removelist);
	}

	public void setCacheProducer(boolean flag){
		cacheproducer = flag;
	}

	public void setCacheConsumer(boolean flag){
		cacheconsumer = flag;
	}

	public void setDepth(int d){
		depth = d;
	}

	public void setExecutable(boolean flag){
		executable = flag;
	}

	public void setLastExecutionTime(long time){
		lastexecutiontime = time;
	}

	public int size(){
		return nodes.size();
	}

	public boolean isCacheProducer(){
		return cacheproducer;
	}

	public boolean isCacheConsumer(){
		return cacheconsumer;
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("<operator ");
		sb.append("type=\"" + getType() + "\" ");
		sb.append("master=\"" + getMasterSet() + "\" ");
		sb.append("source=\"" + getSourceSet() + "\" ");
		sb.append("/>");
		return sb.toString();
	}

}

