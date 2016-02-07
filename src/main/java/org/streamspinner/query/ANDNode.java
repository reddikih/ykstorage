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

import org.streamspinner.Operators;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.util.Arrays;
import java.io.Serializable;

public class ANDNode implements Serializable, Node, Operators { 

	private String type;

	private HashSet qids;

	private MasterSet masters;
	private AttributeList attrs;
	private SourceSet sources;
	private PredicateSet conds;
	private FunctionParameter func;
	private RenameParameter rename;
	private TableManipulationParameter table;
	private HashSet inputs;
	private ORNode output;

	private ANDNode(){
		;
	}

	private ANDNode(String type, MasterSet masters, SourceSet sources, ORNode input, ORNode output){
		this(type, masters, sources, new ORNode[0], output);
		this.inputs.add(input);
	}

	private ANDNode(String type, MasterSet masters, SourceSet sources, ORNode[] inputs, ORNode output){
		this.type = type;
		this.qids = new HashSet();
		if(masters == null || sources == null || inputs == null)
			throw new NullPointerException("Arguments must not be null");
		this.masters = masters;
		this.sources = sources;
		this.inputs = new HashSet(Arrays.asList(inputs));
		this.output = output;

		this.attrs = null;
		this.conds = null;
		this.func= null;
		this.rename = null;
		this.table = null;
	}

	public static ANDNode createRoot(MasterSet masters, SourceSet sources, ORNode input){
		return new ANDNode(ROOT, masters, sources, input, null);
	}


	public static ANDNode createSelection(MasterSet masters, SourceSet sources, PredicateSet conds, ORNode input, ORNode output){
		if(conds == null || output == null)
			throw new NullPointerException("Arguments must not be null");
		ANDNode rval = new ANDNode(SELECTION, masters, sources, input, output);
		rval.conds = conds;
		return rval;
	}

	public static ANDNode createJoin(MasterSet masters, SourceSet sources, PredicateSet conds, ORNode[] inputs, ORNode output){
		if(conds == null || output == null)
			throw new NullPointerException("Arguments must not be null");
		ANDNode rval = new ANDNode(JOIN, masters, sources, inputs, output);
		rval.conds = conds;
		return rval;
	}

	public static ANDNode createProjection(MasterSet masters, SourceSet sources, AttributeList attrs, ORNode input, ORNode output){
		if(attrs == null || output == null)
			throw new NullPointerException("Arguments must not be null");
		ANDNode rval = new ANDNode(PROJECTION, masters, sources, input, output);
		rval.attrs = attrs;
		return rval;
	}

	public static ANDNode createEval(MasterSet masters, SourceSet sources, FunctionParameter func, ORNode input, ORNode output){
		if(func == null || output == null)
			throw new NullPointerException("Arguments must not be null");
		ANDNode rval = new ANDNode(EVAL, masters, sources, input, output);
		rval.func = func;
		return rval;
	}

	public static ANDNode createGroup(MasterSet masters, SourceSet sources, AttributeList attrs, ORNode input, ORNode output){
		if(attrs == null)
			throw new NullPointerException("Arguments must not be null");
		ANDNode rval = new ANDNode(GROUP, masters, sources, input, output);
		rval.attrs = attrs;
		return rval;
	}

	public static ANDNode createRename(MasterSet masters, SourceSet sources, RenameParameter rp, ORNode input ,ORNode output){
		if(rp == null)
			throw new NullPointerException("Arguments must not be null");
		ANDNode rval = new ANDNode(RENAME, masters, sources, input, output);
		rval.rename = rp;
		return rval;
	}

	public static ANDNode createStore(MasterSet masters, SourceSet sources, TableManipulationParameter tp, ORNode input ,ORNode output){
		if(tp == null)
			throw new NullPointerException("Arguments must not be null");
		ANDNode rval = new ANDNode(STORE, masters, sources, input, output);
		rval.table = tp;
		return rval;
	}

	public static ANDNode createTableCreate(MasterSet masters, SourceSet sources, TableManipulationParameter tp, ORNode output){
		if(tp == null)
			throw new NullPointerException("Arguments must not be null");
		ANDNode rval = new ANDNode(CREATE, masters, sources, new ORNode[0], output);
		rval.table = tp;
		return rval;
	}

	public static ANDNode createTableDrop(MasterSet masters, SourceSet sources, TableManipulationParameter tp, ORNode output){
		if(tp == null)
			throw new NullPointerException("Arguments must not be null");
		ANDNode rval = new ANDNode(DROP, masters, sources, new ORNode[0], output);
		rval.table = tp;
		return rval;
	}

	public void addQueryID(Object qid){
		qids.add(qid);
	}

	public void clearQueryID(){
		qids = new HashSet();
	}

	public boolean equals(Object o){
		if(! (o instanceof ANDNode))
			return false;
		ANDNode target = (ANDNode)o;

		if(! type.equals(target.getType()))
			return false;
		if(! masters.equals(target.getMasterSet()))
			return false;
		if(! sources.equals(target.getSourceSet()))
			return false;

		if( (attrs == null && target.attrs != null) || (attrs != null && (! attrs.equals(target.attrs))) )
			return false;

		if( (conds == null && target.conds != null) || (conds != null && (! conds.equals(target.conds))) )
			return false;

		if( (func == null && target.func != null) || (func != null && (! func.equals(target.func))) )
			return false;

		if((rename == null && target.rename != null) || (rename != null && (! rename.equals(target.rename))) )
			return false;

		if((table == null && target.table != null) || (table != null && (! table.equals(target.table))) )
			return false;

		if(inputs.size() != target.inputs.size())
			return false;
		for(Iterator it = inputs.iterator(); it.hasNext(); )
			if(! target.inputs.contains(it.next()))
				return false;

		if( (output == null && target.output != null) || (output != null && (! output.equals(target.output))) )
			return false;

		return true;
	}


	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("<ANDNode type=\"");
		sb.append(type);
		sb.append("\" ");
		sb.append("query=\"");
		for(Iterator it = qids.iterator(); it.hasNext(); ){
			sb.append(it.next());
			if(it.hasNext())
				sb.append(",");
		}
		sb.append("\" ");
		sb.append("master=\"");
		sb.append(masters.toString());
		sb.append("\" ");
		sb.append("source=\"");
		sb.append(sources.toString());
		sb.append("\" ");
		if(attrs != null){
			sb.append("attr=\"");
			sb.append(attrs.toString());
			sb.append("\" ");
		}
		if(conds != null){
			sb.append("conds=\"");
			sb.append(conds.toString());
			sb.append("\" ");
		}
		if(func != null){
			sb.append("func=\"");
			sb.append(func.toString());
			sb.append("\"");
		}
		if(rename != null){
			sb.append("rename=\"");
			sb.append(rename.toString());
			sb.append("\"");
		}
		if(table != null){
			sb.append("table=\"");
			sb.append(table.toString());
			sb.append("\"");
		}
		if(inputs != null){
			for(Iterator it = inputs.iterator(); it.hasNext(); ){
				sb.append("input=\"");
				ORNode on = (ORNode)(it.next());
				sb.append(on.toString());
				sb.append("\" ");
			}
		}
		if(output != null){
			sb.append("output=\"");
			sb.append(output.toString());
			sb.append("\" ");
		}
		sb.append("/>");
		return sb.toString();
	}

	public int hashCode(){
		int rval = 0;
		if(masters != null)
			rval += masters.hashCode();
		if(attrs != null)
			rval += attrs.hashCode();
		if(sources != null)
			rval += sources.hashCode();
		if(conds != null)
			rval += conds.hashCode();
		if(func != null)
			rval += func.hashCode();
		if(rename != null)
			rval += rename.hashCode();
		if(table != null)
			rval += table.hashCode();
		if(inputs != null){
			for(Iterator it = inputs.iterator(); it.hasNext(); )
				rval += it.next().hashCode();
		}
		if(output != null)
			rval += output.hashCode();

		return rval;
	}


	public ANDNode copy(){
		ANDNode rval = new ANDNode();
		rval.type = new String(type);
		rval.qids = (HashSet)qids.clone();
		rval.masters = masters.copy();

		if(sources != null)
			rval.sources = sources.copy();
		if(conds != null)
			rval.conds = conds.copy();
		if(attrs != null)
			rval.attrs = attrs.copy();
		if(func != null)
			rval.func = func.copy();
		if(rename != null)
			rval.rename = rename.copy();
		if(table != null)
			rval.table = table.copy();
		if(inputs != null){
			rval.inputs = new HashSet();
			for(Iterator it = inputs.iterator(); it.hasNext(); )
				rval.inputs.add(it.next());
		}
		if(output != output)
			rval.output = output.copy();

		return rval;
	}

	public String getType(){
		return type;
	}

	public PredicateSet getPredicateSet(){
		return conds;
	}

	public MasterSet getMasterSet(){
		return masters;
	}

	public SourceSet getSourceSet(){
		return sources;
	}

	public AttributeList getAttributeList(){
		return attrs;
	}

	public FunctionParameter getFunctionParameter(){
		return func;
	}

	public RenameParameter getRenameParameter(){
		return rename;
	}

	public TableManipulationParameter getTableManipulationParameter(){
		return table;
	}

	public ORNode[] getInputORNodes(){
		ORNode[] rval = new ORNode[inputs.size()];
		rval = (ORNode[])(inputs.toArray(rval));
		return rval;
	}

	public ORNode getOutputORNode(){
		return output;
	}

	public int getSharedCount(){
		return qids.size();
	}

	public boolean isANDNode(){
		return true;
	}

	public boolean isORNode(){
		return false;
	}

	public Set getQueryIDSet(){
		return qids;
	}

	public void removeQueryID(Object qid){
		qids.remove(qid);
	}

}
