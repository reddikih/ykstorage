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
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.HashMap;

public class DefaultExecutionPlan implements ExecutionPlan {

	private static int counter = 0;

	private String planid;
	private HashSet queryid;
	private MasterSet masters;
	private OperatorGroup[] groups;
	private ORNode[] ornodes;
	private HashMap trigger;
	private HashMap bases;

	public DefaultExecutionPlan(DAG d){
		planid = new String("DEFAULT-" + counter);
		counter ++;

		queryid = new HashSet();
		bases = new HashMap();

		masters = new MasterSet();
		ANDNode[] roots = d.getRootNodes();
		for(int i=0; i < roots.length; i++){
			queryid.addAll(roots[i].getQueryIDSet());
			masters = masters.concat(roots[i].getMasterSet());
		}

		createPlan(d, roots);

		ArrayList nodes = new ArrayList();
		for(int i=0; i < groups.length; i++){
			ORNode[] inputs = groups[i].getInputORNodes();
			for(int j=0; j < inputs.length; j++){
				nodes.add(inputs[j]);
				if(inputs[j].isBase())
					bases.put(inputs[j].getSources().iterator().next(), inputs[j]);
			}
		}
		ornodes = (ORNode[])(nodes.toArray(new ORNode[0]));

		trigger = new HashMap();
		for(int i=0; i < groups.length; i++){
			MasterSet ms = groups[i].getMasterSet();
			for(Iterator it = ms.iterator(); it.hasNext(); ){
				Object m = it.next();
				if(trigger.containsKey(m)){
					ArrayList l = (ArrayList)(trigger.get(m));
					l.add(groups[i]);
				}
				else {
					ArrayList l = new ArrayList();
					l.add(groups[i]);
					trigger.put(m, l);
				}
			}
		}
	}

	private void createPlan(DAG d, ANDNode[] roots){
		ArrayList plans = new ArrayList();
		LinkTable outputlink = d.getOutputLinkTable();

		for(int i=0; i < roots.length; i++){
			ArrayList plan = new ArrayList();
			traverse(plan, roots[i], outputlink);
			Iterator it = plan.iterator();
			while(it.hasNext()){
				OperatorGroup og = (OperatorGroup)(it.next());
				if(! plans.contains(og))
					plans.add(og);
			}
		}
		groups = (OperatorGroup[])(plans.toArray(new OperatorGroup[0]));
	}

	private int traverse(ArrayList plan, ANDNode target, LinkTable outputlink){
		ORNode[] inputs = target.getInputORNodes();
		int childdepth = 0;
		for(int i=0; i < inputs.length; i++){
			if(outputlink.containsKey(inputs[i])){
				Node[] tmp = outputlink.lookup(inputs[i]);
				ANDNode[] candidates = new ANDNode[tmp.length];
				System.arraycopy(tmp, 0, candidates, 0, tmp.length);
				ANDNode selected = select(candidates);
				int depth = traverse(plan, selected, outputlink);
				childdepth = Math.max(depth, childdepth);
			}
		}
		OperatorGroup og = new OperatorGroup(target);
		og.setDepth(childdepth + 1);
		plan.add(og);
		return og.getDepth();
	}

	private ANDNode select(ANDNode[] candidates){
		if(candidates.length == 1)
			return candidates[0];

		ANDNode best = candidates[0];
		for(int i=1; i < candidates.length; i++){
			if((! best.getType().equals(ANDNode.JOIN)) && candidates[i].getType().equals(ANDNode.JOIN)){
				best = candidates[i];
				continue;
			}
		}
		return best;
	}

	public ORNode[] getORNodes(){
		return ornodes;
	}

	public boolean isTriggered(String master){
		return masters.contains(master);
	}

	public String[] getTriggers(){
		ArrayList l = new ArrayList();
		for(Iterator it=masters.iterator(); it.hasNext(); )
			l.add(it.next());
		return (String[])(l.toArray(new String[0]));
	}

	public OperatorGroup[] getOperators(){
		return groups;
	}

	public OperatorGroup[] getOperatorsOnMaster(String master){
		if(trigger.containsKey(master)){
			ArrayList l = (ArrayList)(trigger.get(master));
			return (OperatorGroup[])(l.toArray(new OperatorGroup[0]));
		}
		else
			return null;
	}

	public Set getQueryIDSet(){
		return queryid; 
	}

	public String getPlanID(){
		return planid;
	}

	public ORNode getBaseORNode(String source){
		if(bases.containsKey(source))
			return (ORNode)(bases.get(source));
		else
			return null;
	}
}
