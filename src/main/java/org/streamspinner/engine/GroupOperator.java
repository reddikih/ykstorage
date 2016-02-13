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
package org.streamspinner.engine;

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.DataTypes;
import org.streamspinner.Operators;
import org.streamspinner.LogEntry;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.AttributeList;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;


public abstract class GroupOperator extends Operator {

	public static LogEntry process(long executiontime, ExecutionPlan plan, OperatorGroup op, QueueManager qm, CacheManager cm) throws StreamSpinnerException {
		ANDNode[] node = op.getANDNodes();

		ORNode inputn = node[0].getInputORNodes()[0];
		Queue inputq = qm.getQueue(plan, op.getMasterSet(), inputn);

		Queue[] outputq = new Queue[node.length];
		for(int i=0; i < node.length; i++){
			ORNode outputn = node[i].getOutputORNode();
			outputq[i] = qm.getQueue(plan, node[i].getMasterSet(), outputn);
			if(outputq[i] == null)
				throw new StreamSpinnerException("no input queue for " + outputn.toString());
		}

		TupleSet ts = getDeltaTupleSet(executiontime, plan, op, inputn, inputq, cm);

		LogEntry le = null;
		if(op.isCacheConsumer() == true)
			le = consumeCacheData(executiontime, plan, op, qm, cm);
		else
			le = new LogEntry(executiontime, op);

		if(ts.first() == false)
			return le;

		AttributeList key = node[0].getAttributeList();
		Schema oschema = outputq[0].getSchema();
		GroupGenerator gg = new GroupGenerator(oschema, key);
		List<Tuple> tlist = gg.createGroups(ts);

		OnMemoryTupleSet result = new OnMemoryTupleSet(oschema);
		for(Tuple t : tlist ){
			for(int i=0; i < node.length; i++)
				outputq[i].append(t);
			result.append(t);
			le.add(t);
		}

		ts.close();
		produceCacheData(executiontime, plan, op, cm, result);
		inputq.moveToWindow(op, executiontime);
		return le;
	}

	private static class GroupGenerator {

		private HashMap<Map<Integer, Object>, GroupTable> groups;
		private int[] keyindexes;
		private int[] valueindexes;
		private Schema schema;
		private AttributeList attrs;

		private GroupGenerator(Schema s, AttributeList a) throws StreamSpinnerException {
			schema = s;
			attrs = a;
			checkAttributeList();
			getIndexes();
			groups = new HashMap<Map<Integer, Object>, GroupTable>();
		}

		private void checkAttributeList() throws StreamSpinnerException {
			try {
				if(attrs.size() == 1 && attrs.getString(0).equals(""))
					attrs = new AttributeList();
				for(int i=0; i < attrs.size(); i++)
					schema.getIndex(attrs.getString(i));
			} catch(IllegalArgumentException iae){
				throw new StreamSpinnerException(iae);
			}
		}

		private void getIndexes() throws StreamSpinnerException {
			keyindexes = new int[attrs.size()];
			valueindexes = new int[schema.size() - attrs.size()];
			int kcount = 0, vcount = 0;
			for(int i=0; i < schema.size(); i++){
				boolean found = false;
				for(int j=0; j < attrs.size(); j++){
					if(schema.getAttributeName(i).equals(attrs.getString(j))){
						found = true;
						break;
					}
				}
				if(found){
					keyindexes[kcount] = i;
					kcount++;
				}
				else {
					valueindexes[vcount] = i;
					vcount++;
				}
			}
		}

		private Map<Integer, Object> getKeyMap(Tuple t) throws StreamSpinnerException {
			Map<Integer, Object> rval = new HashMap<Integer, Object>();
			for(int i=0; i < keyindexes.length; i++)
				rval.put(keyindexes[i], t.getObject(keyindexes[i]));
			return rval;
		}

		private List<Tuple> createGroups(TupleSet ts) throws StreamSpinnerException {
			Map<Integer, Object> key=null;
			do {
				Tuple t = ts.getTuple();
				key = getKeyMap(t);
				if(! groups.containsKey(key))
					groups.put(key, new GroupTable(schema, key));
				GroupTable gt = groups.get(key);
				gt.add(t);
			} while(ts.next());

			Collection<GroupTable> gc = groups.values();
			List<Tuple> rval = new ArrayList<Tuple>(gc.size());
			for(GroupTable gt : gc )
				rval.add(gt.getTuple());
			return rval;
		}
	}


	private static class GroupTable {

		private HashMap<String, Long> timestamps;
		private Map<Integer, Object> keymap;
		private HashMap<Integer, List<Object>> columnmap;
		private Schema schema;

		private GroupTable(Schema s, Map<Integer, Object> key){
			schema = s;
			keymap = key;

			timestamps = new HashMap<String, Long>();
			for(String tablename : s.getBaseTableNames())
				timestamps.put(tablename, Long.MIN_VALUE);

			columnmap = new HashMap<Integer, List<Object>>();
			for(int i=0; i < s.size(); i++)
				if(! keymap.containsKey(i))
					columnmap.put(i, new ArrayList<Object>());
		}

		private void add(Tuple t) throws StreamSpinnerException {
			for(String tablename : timestamps.keySet()){
				long current = timestamps.get(tablename);
				long newtime = t.getTimestamp(tablename);
				if(newtime > current)
					timestamps.put(tablename, newtime);
			}
			for(int index : columnmap.keySet()){
				List<Object> column = columnmap.get(index);
				column.add(t.getObject(index));
			}
		}

		private Tuple getTuple() throws StreamSpinnerException {
			Tuple rval = new Tuple(schema.size());
			for(String tablename : timestamps.keySet()){
				long timestamp = timestamps.get(tablename);
				if(timestamp != Long.MIN_VALUE)
					rval.setTimestamp(tablename, timestamp);
			}

			for(int i=0; i < schema.size(); i++){
				if(keymap.containsKey(i))
					rval.setObject(i, keymap.get(i));
				else if(columnmap.containsKey(i)){
					List<Object> column = columnmap.get(i);
					String type = schema.getType(i);
					if(type.equals(DataTypes.ARRAY_STRING))
						rval.setObject(i, column.toArray(new String[0]));
					else if(type.equals(DataTypes.ARRAY_LONG))
						rval.setObject(i, toLongArray(column));
					else if(type.equals(DataTypes.ARRAY_DOUBLE))
						rval.setObject(i, toDoubleArray(column));
					else
						rval.setObject(i, column.toArray());
				}
			}
			return rval;
		}
	}

	private static long[] toLongArray(List<Object> column){
		long[] array = new long[column.size()];
		for(int i=0; i < column.size(); i++)
			array[i] = ((Long)(column.get(i))).longValue();
		return array;
	}

	private static double[] toDoubleArray(List<Object> column){
		double[] array = new double[column.size()];
		for(int i=0; i < column.size(); i++)
			array[i] = ((Double)(column.get(i))).doubleValue();
		return array;
	}


}

