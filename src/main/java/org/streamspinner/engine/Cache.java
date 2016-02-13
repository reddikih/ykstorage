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
import org.streamspinner.query.OperatorGroup;
import org.streamspinner.query.SourceSet;
import java.util.Iterator;
import java.util.ArrayList;

public class Cache {

	private static double threshold = 0.0;

	public static void setThreshold(double t){
		threshold = t;
	}

	public static double getThreshold(){
		return threshold;
	}

	private Object cacheid;
	private Schema schema;
	private ArrayList<Tuple> data;
	private long creationtime;
	private OperatorGroup creator;

	public Cache(Object cid){
		cacheid = cid;
		creationtime = 0;
		schema = null;
		data = new ArrayList<Tuple>();
		creator = null;
	}

	public int hashCode(){
		return cacheid.hashCode();
	}

	public boolean equals(Object target){
		if(! ( target instanceof Cache))
			return false;
		Cache c = (Cache)(target);
		return cacheid.equals(c.cacheid);
	}

	public Object getCacheID(){
		return cacheid;
	}

	public long getCreationTime(){
		return creationtime;
	}

	public OperatorGroup getCreator(){
		return creator;
	}

	public TupleSet getTupleSet(){
		return new OnMemoryTupleSet(schema, data);
	}

	public TupleSet getTupleSet(long executiontime, OperatorGroup op) throws StreamSpinnerException {
		SourceSet ss = op.getSourceSet();
		long lastexecutiontime = op.getLastExecutionTime();
		ArrayList<Tuple> rlist = new ArrayList<Tuple>(data.size());
		for(Tuple t : data){
			if(t.isIncludedWindow(executiontime, ss) && t.isNewerThan(lastexecutiontime))
				rlist.add(t);
		}
		OnMemoryTupleSet rval = new OnMemoryTupleSet(schema, rlist);
		return rval;
	}

	public boolean isReusable(long executiontime, OperatorGroup op) throws StreamSpinnerException {
		if(hasData() == false)
			return false;
		if(creator.equals(op))
			return false;
		SourceSet ss = op.getSourceSet();
		SourceSet creatorss = creator.getSourceSet();
		for(Iterator it = ss.iterator(); it.hasNext(); ){
			Object source = it.next();
			long creatorwin = creatorss.getWindowsize(source);
			long cachewindow = creationtime - creatorwin;
			long targetwindow = executiontime - ss.getWindowsize(source);
			if(targetwindow < cachewindow || creationtime < targetwindow)
				return false;
			double overlap = (double)(creationtime - targetwindow) / (double)(creatorwin);
			if(overlap < threshold)
				return false;
		}
		return true;
	}
	
	public void updateCache(long time, OperatorGroup op, TupleSet ts) throws StreamSpinnerException {
		creationtime = time;
		creator = op;
		schema = ts.getSchema();
		if(data == null)
			data = new ArrayList<Tuple>();
		ts.beforeFirst();
		while(ts.next())
			data.add(ts.getTuple());
	}

	public boolean hasData(){
		return data != null && data.size() != 0;
	}

	public void clean(){
		creationtime = 0;
		creator = null;
		data = new ArrayList<Tuple>();
		schema = null;
	}

	public void collectGarbage(long executiontime){
		if(hasData() == false)
			return;
		ArrayList<Tuple> newdata = new ArrayList<Tuple>(data.size());
		for(Tuple t : data){
			if(t.isIncludedWindow(executiontime, creator.getSourceSet()))
				newdata.add(t);
		}
		data = newdata;
	}


	public String toString(){
		StringBuffer sb = new StringBuffer();
		if(data == null || data.size() == 0)
			return sb.toString();
		for(int i=0; i < data.size(); i++)
			sb.append(data.get(i).toString());
		return sb.toString();
	}

}
