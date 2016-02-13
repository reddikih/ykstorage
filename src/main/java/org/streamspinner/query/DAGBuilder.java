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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

public class DAGBuilder {

	private static DAGBuilder instance = null;

	HashSet andnodes;
	HashSet ornodes;
	LinkTable inputs;
	LinkTable outputs;

	private DAGBuilder(){
		;
	}

	public DAG createDAG(String qstr) throws StreamSpinnerException {
		QueryParser qp = new QueryParser();
		Query q = qp.parse(qstr);
		return createDAG(q);
	}

	public DAG createDAG(Query q){
		analyze(q);

		ORNode[] onodes = (ORNode[])(ornodes.toArray(new ORNode[0]));
		ANDNode[] anodes = (ANDNode[])(andnodes.toArray(new ANDNode[0]));
		DAG rval = new DAG(anodes, onodes, inputs, outputs);

		return rval;
	}


	private void analyze(Query q){
		andnodes = new HashSet();
		ornodes = new HashSet();
		inputs = new LinkTable();
		outputs = new LinkTable();

		createRoot(q);
		createProjection(q);
		createSelection(q);
		createJoin(q);

		for(Iterator it = andnodes.iterator(); it.hasNext(); ){
			ANDNode n = (ANDNode)(it.next());
			n.addQueryID(q.getID());
		}
	}


	private void createRoot(Query q){
		ORNode input = new ORNode(q.getSelectClause(), q.getFromClause(), q.getWhereClause());
		ANDNode root = ANDNode.createRoot(q.getMasterClause(), q.getFromClause(), input);
		if(! ornodes.contains(input))
			ornodes.add(input);
		if(! andnodes.contains(root)){
			andnodes.add(root);
			inputs.add(input, root);
		}
	}

	private void createProjection(Query q){
		AttributeList attrs = q.getSelectClause();
		if(attrs.size() == 1 && attrs.getString(0).equals("*"))
			return;

		SourceSet sources = q.getFromClause();
		PredicateSet conds = q.getWhereClause();
		AttributeList any = new AttributeList("*");

		ORNode input = new ORNode(any, sources, conds);
		ORNode output = new ORNode(attrs, sources, conds);
		if(! ornodes.contains(input))
			ornodes.add(input);
		if(! ornodes.contains(output))
			ornodes.add(output);

		ANDNode projection = ANDNode.createProjection(q.getMasterClause(), sources, attrs, input, output);
		if(! andnodes.contains(projection)){
			andnodes.add(projection);
			inputs.add(input, projection);
			outputs.add(output, projection);
		}
	}


	private void createSelection(Query q){
		MasterSet masters = q.getMasterClause();
		SourceSet sources = q.getFromClause();
		PredicateSet conds = q.getWhereClause();

		AttributeList attrs = new AttributeList("*");

		// Selection operator (Push-down strategy)
		for(Iterator sit = sources.iterator(); sit.hasNext(); ){
			String sname = (String)(sit.next());
			SourceSet ss = new SourceSet(sname, sources.getWindowsize(sname));
			PredicateSet cond = conds.getSelectionConditionsOf(ss);
			PredicateSet empty = new PredicateSet();
			if(cond.size() != 0){
				ORNode input = new ORNode(attrs, ss, empty);
				ORNode output = new ORNode(attrs, ss, cond);
				if(! ornodes.contains(input))
					ornodes.add(input);
				if(! ornodes.contains(output))
					ornodes.add(output);
				ANDNode selection = ANDNode.createSelection(masters, ss, cond, input, output);
				if(! andnodes.contains(selection)){
					andnodes.add(selection);
					inputs.add(input, selection);
					outputs.add(output, selection);
				}
			}
		}

		// Selection operator (Pull-up strategy)
		PredicateSet cond = conds.getSelectionConditionsOf(sources);
		if(cond.size() != 0){
			PredicateSet other = conds.diff(cond);
			ORNode input = new ORNode(attrs, sources, other);
			ORNode output = new ORNode(attrs, sources, conds);
			ANDNode selection = ANDNode.createSelection(masters, sources, cond, input, output); 
			if(! ornodes.contains(input))
				ornodes.add(input);
			if(! ornodes.contains(output))
				ornodes.add(output);
			if(! andnodes.contains(selection)){
				andnodes.add(selection);
				inputs.add(input, selection);
				outputs.add(output, selection);
			}
		}
	}

	private void createJoin(Query q){
		SourceSet sources = q.getFromClause();
		if(sources.size() < 2)
			return;

		ArrayList bases = new ArrayList();
		for(Iterator sit = sources.iterator(); sit.hasNext(); ){
			String sname = (String)(sit.next());
			bases.add(new SourceSet(sname, sources.getWindowsize(sname)));
		}

		for(int i=0; i < sources.size(); i++){
			ArrayList nextbases = new ArrayList();
			for(Iterator bit = bases.iterator(); bit.hasNext(); ){
				SourceSet base = (SourceSet)(bit.next());
				for(Iterator sit = sources.iterator(); sit.hasNext() ; ){
					String sname = (String)(sit.next());
					if(base.contains(sname))
						continue;
					createJoinNode(q, base, sname);
					SourceSet nextbase = base.copy();
					nextbase.add(sname, sources.getWindowsize(sname));
					nextbases.add(nextbase);
				}
			}
			bases = nextbases;
		}
	}

	private void createJoinNode(Query q, SourceSet base, String source){
		long windowsize = q.getFromClause().getWindowsize(source);
		SourceSet target = new SourceSet(source, windowsize);
		SourceSet outputsources = base.copy();
		outputsources.add(source, windowsize);

		PredicateSet orig = q.getWhereClause();
		PredicateSet basecond = orig.getJoinConditionsOf(base);
		PredicateSet outputcond = orig.getJoinConditionsOf(outputsources);
		PredicateSet cond = outputcond.diff(basecond);

		AttributeList any = new AttributeList("*");
		PredicateSet empty = new PredicateSet();

		// Selection pull-up
		{
			ORNode[] input = new ORNode[2];
			input[0] = new ORNode(any, base, basecond);
			input[1] = new ORNode(any, target, empty);
			ORNode output = new ORNode(any, outputsources, outputcond);
			for(int i=0; i < input.length; i++)
				if(! ornodes.contains(input[i]))
					ornodes.add(input[i]);
			if(! ornodes.contains(output))
				ornodes.add(output);
			ANDNode join = ANDNode.createJoin(q.getMasterClause(), outputsources, cond, input, output);
			if(! andnodes.contains(join)){
				andnodes.add(join);
				inputs.add(input[0], join);
				inputs.add(input[1], join);
				outputs.add(output, join);
			}
		}

		// Selection push-down
		basecond = basecond.concat(orig.getSelectionConditionsOf(base));
		outputcond = outputcond.concat(orig.getSelectionConditionsOf(outputsources));
		PredicateSet targetcond = orig.getSelectionConditionsOf(target);
		{
			ORNode[] input = new ORNode[2];
			input[0] = new ORNode(any, base, basecond);
			input[1] = new ORNode(any, target, targetcond);
			if(! ornodes.contains(input[0]))
					ornodes.add(input[0]);
			if(! ornodes.contains(input[1]))
					ornodes.add(input[1]);
			ORNode output = new ORNode(any, outputsources, outputcond);
			if(! ornodes.contains(output))
				ornodes.add(output);
			ANDNode join = ANDNode.createJoin(q.getMasterClause(), outputsources, cond, input, output);
			if(! andnodes.contains(join)){
				andnodes.add(join);
				inputs.add(input[0], join);
				inputs.add(input[1], join);
				outputs.add(output, join);
			}
		}
	}

	public static DAGBuilder getInstance(){
		if(instance == null)
			instance = new DAGBuilder();
		return instance;
	}

}
