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

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.InformationSource;
import org.streamspinner.InformationSourceManager;
import org.streamspinner.Mediator;
import org.streamspinner.SystemManager;
import org.streamspinner.MetaDataUpdateListener;
import org.streamspinner.InternalState;
import org.streamspinner.Recoverable;
import org.streamspinner.engine.Schema;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.SourceSet;
import java.util.Vector;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.Map;


public class InformationSourceManagerImpl implements InformationSourceManager, MetaDataUpdateListener {

	private ArrayList<InformationSource> sources;
	private HashMap<String, InformationSource> names;
	private HashMap<String, InformationSource> tables;
	private boolean running;
	private Mediator mediator = null;
	private SystemManager sysman = null;

	public InformationSourceManagerImpl(String confdir) throws StreamSpinnerException {
		InformationSourceParser isp = new InformationSourceParser();
		sources = new ArrayList(Arrays.asList(isp.parseAll(confdir)));

		names = new HashMap<String,InformationSource>();
		tables = new HashMap<String,InformationSource>();

		initAllInformationSources();

		for(InformationSource is : sources){
			names.put(is.getName(), is);
			String[] tablenames = is.getAllTableNames();
			for(int i=0; tablenames != null && i < tablenames.length; i++)
				tables.put(tablenames[i], is);
			is.addMetaDataUpdateListener(this);
		}
		running = false;
	}

	public InformationSource getInformationSource(String name) throws StreamSpinnerException {
		if(names.containsKey(name))
			return names.get(name);
		else
			throw new StreamSpinnerException("No such information source: " + name);
	}

	public InformationSource[] getAllInformationSources() {
		return sources.toArray(new InformationSource[0]);
	}

	public void initAllInformationSources() throws StreamSpinnerException {
		if(running == true)
			stopAllInformationSources();
		for(InformationSource is : sources)
			is.init();
		running = false;
	}

	public void startAllInformationSources() throws StreamSpinnerException {
		for(InformationSource is : sources)
			is.start();
		running = true;
	}

	public void stopAllInformationSources() throws StreamSpinnerException {
		for(InformationSource is : sources)
			is.stop();
		running = false;
	}

	public InformationSource getSourceFromTableName(String tablename) throws StreamSpinnerException {
		if(tables.containsKey(tablename))
			return tables.get(tablename);
		else
			throw new StreamSpinnerException("No such table : " + tablename);
	}

	public Schema getSchema(ORNode on) throws StreamSpinnerException {
		if(! on.isBase())
			throw new StreamSpinnerException("Cannot extract schema: " + on.toString());
		SourceSet ss = on.getSources();
		String tablename = (String)(ss.iterator().next());
		InformationSource is = tables.get(tablename);
		return is.getSchema(tablename);
	}

	public void addInformationSource(InformationSource is) throws StreamSpinnerException {
		if(names.containsKey(is.getName()))
			throw new StreamSpinnerException("InformationSource named "+is.getName()+" already exists.");

		is.init();

		sources.add(is);
		is.addMetaDataUpdateListener(this);
		names.put(is.getName(), is);

		String[] tablenames = is.getAllTableNames();
		for(int i=0; tablenames != null && i < tablenames.length; i++)
			tables.put(tablenames[i], is);

		if(running == true)
			is.start();
		if(mediator != null)
			is.addArrivalTupleListener(mediator);
		if(sysman != null)
			sysman.informationSourceAdded(is);
	}

	public void addInformationSource(String confstr) throws StreamSpinnerException {
		InformationSourceParser isp = new InformationSourceParser();
		addInformationSource(isp.parseString(confstr));
	}

	public void removeInformationSource(InformationSource is) throws StreamSpinnerException {
		if(! sources.contains(is))
			return;

		InformationSource target = sources.remove(sources.indexOf(is));
		if(target != null){
			if(mediator != null)
				target.removeArrivalTupleListener(mediator);
			if(running == true)
				target.stop();
			names.remove(target.getName());
			String[] tablenames = target.getAllTableNames();
			for(int i=0; tablenames != null && i < tablenames.length; i++)
				tables.remove(tablenames[i]);
			if(sysman != null)
				sysman.informationSourceDeleted(target);
		}
	}

	public void removeInformationSource(String name) throws StreamSpinnerException {
		if(names.containsKey(name))
			removeInformationSource(names.get(name));
	}

	public String[] getAllTableNames() throws StreamSpinnerException {
		Set<String> keys = tables.keySet();
		return keys.toArray(new String[0]);
	}

	public Schema[] getAllSchemas() throws StreamSpinnerException {
		ArrayList<Schema> schemalist = new ArrayList<Schema>();
		for(InformationSource is : sources){
			String[] tablenames = is.getAllTableNames();
			for(int i=0; tablenames != null && i < tablenames.length; i++)
				schemalist.add(is.getSchema(tablenames[i]));
		}
		return schemalist.toArray(new Schema[0]);
	}

	public void setMediator(Mediator m){
		for(InformationSource is : sources){
			if(m == null && mediator != null)
				is.removeArrivalTupleListener(mediator);
			else
				is.addArrivalTupleListener(m);
		}
		mediator = m;
	}

	public void setSystemManager(SystemManager sm){
		sysman = sm;
	}

	public boolean isAcceptable(InternalState state){
		return state instanceof InformationSourceManagerInternalState;
	}

	public InternalState getInternalState(){
		InformationSourceManagerInternalState rval = new InformationSourceManagerInternalState();
		for(InformationSource is : sources){
			if(is instanceof Recoverable){
				Recoverable r = (Recoverable)is;
				rval.addInformationSourceInternalState(is.getName(), r.getInternalState());
			}
		}
		return rval;
	}

	public void setInternalState(InternalState state) throws StreamSpinnerException {
		if(! isAcceptable(state))
			throw new StreamSpinnerException("Unknown state object is given");
		InformationSourceManagerInternalState istate = (InformationSourceManagerInternalState)state;
		Map<String,InternalState> sstates = istate.getInformationSourceInternalStates();
		for(String name : sstates.keySet()){
			InformationSource is = getInformationSource(name);
			if(is instanceof Recoverable){
				Recoverable r = (Recoverable)is;
				r.setInternalState(sstates.get(name));
			}
		}
	}


	public void tableCreated(String wrappername, String tablename, Schema schema){
		try {
			InformationSource is = getInformationSource(wrappername);
			if(names.containsKey(tablename))
				throw new StreamSpinnerException("Warning: Table '"+tablename+"' already exists");
			tables.put(tablename, is);
			if(sysman != null)
				sysman.tableCreated(wrappername, tablename, schema);
		} catch(StreamSpinnerException sse){
			sse.printStackTrace();
		}
	}

	public void tableDropped(String wrappername, String tablename){
		tables.remove(tablename);
		if(sysman != null)
			sysman.tableDropped(wrappername, tablename);
	}
}


