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
package org.streamspinner.wrapper;

import org.streamspinner.InformationSource;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.ArrivalTupleListener;
import org.streamspinner.MetaDataUpdateListener;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.engine.Schema;
import org.streamspinner.query.ORNode;
import java.util.Hashtable;
import java.util.Vector;
import java.util.Map;
import java.util.Map.Entry;


public abstract class Wrapper implements InformationSource {

	private String name;
	private Hashtable<String,String> params;
	private Hashtable<ArrivalTupleListener, TuplePublisherThread> atlisteners;
	private Vector<MetaDataUpdateListener> mdulisteners;

	public Wrapper(String name) throws StreamSpinnerException {
		this.name = name;
		this.params = new Hashtable<String,String>();
		this.atlisteners = new Hashtable<ArrivalTupleListener, TuplePublisherThread>();
		this.mdulisteners = new Vector<MetaDataUpdateListener>();
	}

	public void addArrivalTupleListener(ArrivalTupleListener atl){
		TuplePublisherThread tpt = new TuplePublisherThread(atl);
		tpt.startQueueMonitoring();
		atlisteners.put(atl, tpt);
	}

	public void addMetaDataUpdateListener(MetaDataUpdateListener mdul){
		mdulisteners.add(mdul);
	}

	public String getName(){
		return name;
	}

	public String getParameter(String key){
		if(params.containsKey(key))
			return params.get(key);
		else
			return null;
	}

	public void removeArrivalTupleListener(ArrivalTupleListener atl){
		if(atlisteners.containsKey(atl)){
			TuplePublisherThread tpt = atlisteners.get(atl);
			tpt.stopQueueMonitoring();
			atlisteners.remove(atl);
		}
	}

	public void removeMetaDataUpdateListener(MetaDataUpdateListener mdul){
		mdulisteners.remove(mdul);
	}

	public void setParameter(String key, String value){
		params.put(key, value);
	}

	protected void deliverTupleSet(long executiontime, String source, TupleSet ts){
		for(TuplePublisherThread tpt : atlisteners.values())
			tpt.addTupleSet(executiontime, source, ts);
	}

	protected void notifyTableCreation(String tablename, Schema schema){
		for(int i=0; i < mdulisteners.size(); i++){
			MetaDataUpdateListener mdul = mdulisteners.get(i);
			Thread thread = new MetaDataUpdateNotificationThread(mdul, getName(), tablename, schema);
			thread.start();
		}
	}

	protected void notifyTableDrop(String tablename){
		for(int i=0; i < mdulisteners.size(); i++){
			MetaDataUpdateListener mdul = mdulisteners.get(i);
			Thread thread = new MetaDataUpdateNotificationThread(mdul, getName(), tablename);
			thread.start();
		}
	}

	public String toString(){
		StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		sb.append("<wrapper name=\"");
		sb.append(this.getName());
		sb.append("\" class=\"");
		sb.append(this.getClass().getCanonicalName());
		sb.append("\">\n");
		for(Map.Entry<String, String> e : params.entrySet()){
			sb.append("  <parameter name=\"");
			sb.append(e.getKey());
			sb.append("\" value=\"");
			sb.append(e.getValue());
			sb.append("\" />\n");
		}
		sb.append("</wrapper>");
		return sb.toString();
	}

	public abstract void init() throws StreamSpinnerException ;

	public abstract String[] getAllTableNames();

	public abstract Schema getSchema(String tablename);

	public abstract TupleSet getTupleSet(ORNode node) throws StreamSpinnerException ;

	public abstract void start() throws StreamSpinnerException ;

	public abstract void stop() throws StreamSpinnerException ;

}

