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
package org.streamspinner.optimizer;

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.query.Query;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.DefaultExecutionPlan;
import org.streamspinner.query.DAG;
import org.streamspinner.query.DAGBuilder;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.OperatorGroup;
import java.util.*;


public class OptimizedExecutionPlan implements ExecutionPlan {

	private static int counter = 0;
	
	private String planid;
	private Hashtable<String, Vector<OperatorGroup>> mastertable;
	private Vector<OperatorGroup> operators;
	private Vector<Object> queryids;
	private Vector<ORNode> ornodes;
	private Hashtable<String, ORNode> baseornodes;
	private boolean ignoreMaster = false;

	public OptimizedExecutionPlan(){
		planid = new String("Optimized-" + counter++);
		mastertable = new Hashtable<String, Vector<OperatorGroup>>();
		operators = new Vector<OperatorGroup>();
		queryids = new Vector<Object>();
		ornodes = new Vector<ORNode>();
		baseornodes = new Hashtable<String, ORNode>();
		ignoreMaster = false;
	}

	public void addQuery(Query q, DAG d) throws StreamSpinnerException {
		DefaultExecutionPlan dep = new DefaultExecutionPlan(d);

		for(OperatorGroup og : dep.getOperators()){
			OperatorGroup target = addOperatorGroup(operators, og);
			for(Iterator it = target.getMasterSet().iterator(); it.hasNext(); ){
				String trigger = (String)(it.next());
				if(! mastertable.containsKey(trigger))
					mastertable.put(trigger, new Vector<OperatorGroup>());
				Vector<OperatorGroup> oglist = mastertable.get(trigger);
				if(! oglist.contains(target))
					oglist.add(target);
				Collections.sort(oglist);
			}
		}
		Collections.sort(operators);

		for(ORNode on : dep.getORNodes()){
			ornodes.add(on);
			if(on.isBase() == true)
				baseornodes.put(on.getSources().toString(), on);
		}

		queryids.add(q.getID());
	}

	private OperatorGroup addOperatorGroup(List<OperatorGroup> oglist, OperatorGroup target) throws StreamSpinnerException {
		for(OperatorGroup o : oglist){
			if(o.isCommonOperator(target, ignoreMaster)){
				o.add(target);
				return o;
			}
		}
		oglist.add(target);
		return target;
	}

	public void deleteQuery(Query q){
		synchronized(operators){
			for(int i=operators.size()-1; i >= 0; i--){
				OperatorGroup og = operators.get(i);
				og.remove(q);
				if(og.size() <= 0)
					operators.remove(i);
			}
		}
		synchronized(mastertable){
			ArrayList<String> keys = new ArrayList<String>(mastertable.keySet());
			for(String trigger : keys){
				Vector<OperatorGroup> oglist = mastertable.get(trigger);
				for(int i=oglist.size()-1; i >= 0; i--){
					OperatorGroup og = oglist.get(i);
					og.remove(q);
					if(og.size() <= 0)
						oglist.remove(i);
				}
				if(oglist.size() <= 0)
					mastertable.remove(trigger);
			}
		}
		queryids.remove(q.getID());
	}

	public ORNode getBaseORNode(String source){
		if(baseornodes.containsKey(source))
			return baseornodes.get(source);
		else
			return null;
	}

	public OperatorGroup[] getOperators(){
		return operators.toArray(new OperatorGroup[0]);
	}

	public OperatorGroup[] getOperatorsOnMaster(String master){
		if(! isTriggered(master))
			return null;
		Vector<OperatorGroup> oglist = mastertable.get(master);
		return oglist.toArray(new OperatorGroup[0]);
	}

	public String getPlanID(){
		return planid;
	}

	public Set getQueryIDSet(){
		return new HashSet<Object>(queryids);
	}

	public ORNode[] getORNodes(){
		return ornodes.toArray(new ORNode[0]);
	}

	public String[] getTriggers(){
		return mastertable.keySet().toArray(new String[0]);
	}

	public boolean isTriggered(String master){
		return mastertable.containsKey(master);
	}

	public void enableIgnoringMaster(boolean flag){
		ignoreMaster = flag;
	}

	public boolean isIgnoringMaster(){
		return ignoreMaster;
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("<plan>\n");
		sb.append("  <pid value=\"" + planid.toString() + "\" />\n");
		sb.append("  <qid value=\"");
		for(int i=0; i < queryids.size(); i++){
			sb.append(queryids.get(i).toString());
			if(i + 1 < queryids.size())
				sb.append(",");
		}
		sb.append("\" />\n");
		for(int i=0; i < operators.size(); i++){
			OperatorGroup op = operators.get(i);
			sb.append("  ");
			sb.append(op.toString());
			sb.append("\n");
		}
		sb.append("</plan>\n");
		return sb.toString();
	}

}
