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
import java.util.ArrayList;
import java.util.Iterator;

public class DAG {

	private ANDNode[] andnodes;
	private ORNode[] ornodes;
	private LinkTable input;
	private LinkTable output;

	private DAG(){
		;
	}

	protected DAG(ANDNode[] anodes, ORNode[] onodes, LinkTable in, LinkTable out){
		this.andnodes = anodes;
		this.ornodes = onodes;
		this.input = in;
		this.output = out;

	}

	public void addQueryID(Query q){
		for(int i=0; i < andnodes.length; i++)
			andnodes[i].addQueryID(q.getID());
	}

	public void clearQueryID(){
		for(int i=0; i < andnodes.length; i++)
			andnodes[i].clearQueryID();
	}

	public void removeQueryID(Query q){
		for(int i=0; i < andnodes.length; i++)
			andnodes[i].removeQueryID(q.getID());
	}

	public ANDNode[] getANDNodes(){
		return andnodes;
	}

	public ANDNode[] getRootNodes(){
		ArrayList roots = new ArrayList();
		for(int i=0; i < andnodes.length; i++)
			if(andnodes[i].getType().equals(ANDNode.ROOT))
				roots.add(andnodes[i]);
		return (ANDNode[])(roots.toArray(new ANDNode[0]));
	}

	public ORNode[] getORNodes(){
		return ornodes;
	}

	public LinkTable getInputLinkTable(){
		return input;
	}

	public LinkTable getOutputLinkTable(){
		return output;
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("***************** ORNode ******************\n");
		for(int i=0; i < ornodes.length; i++){
			sb.append("<ORNode value=\"");
			sb.append(ornodes[i].toString());
			sb.append("\" />\n");
		}
		sb.append("***************** ANDNode ******************\n");
		for(int i=0; i < andnodes.length; i++){
			sb.append(andnodes[i].toString());
			sb.append("\n");
		}

		sb.append("***************** InputLink ****************\n");
		sb.append(input.toString());
		sb.append("***************** OutputLink ***************\n");
		sb.append(output.toString());

		return sb.toString();
	}

	public Query[] toQueries(){
		ArrayList qlist = new ArrayList();
		for(int i=0; i < andnodes.length; i++){
			if(! andnodes[i].getType().equals(Operators.ROOT))
				continue;
			ORNode oi = andnodes[i].getInputORNodes()[0];
			MasterSet master = andnodes[i].getMasterSet();
			AttributeList select = oi.getAttributeList();
			SourceSet from = oi.getSources().copy();
			SourceSet window = andnodes[i].getSourceSet();
			for(Iterator it = window.iterator(); it.hasNext(); ){
				String s = (String)(it.next());
				from.add(s, window.getWindowsize(s), window.getWindowOrigin(s));
			}
			PredicateSet where = oi.getConditions();
			AttributeList key = oi.getGroupingKeys();
			RenameParameter rename = oi.getRenameParameter();
			TableManipulationParameter table = oi.getTableManipulationParameter();
			qlist.add(new Query(master, select, from, where, key, rename, table));
		}
		return (Query[])(qlist.toArray(new Query[0]));
	}

}

