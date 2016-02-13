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

import org.streamspinner.ArrivalTupleListener;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.engine.Schema;
import org.streamspinner.engine.Tuple;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.engine.OnMemoryTupleSet;
import org.streamspinner.wrapper.Wrapper;
import org.streamspinner.query.ORNode;
import org.streamspinner.connection.CQRowSet;
import org.streamspinner.connection.CQRowSetMetaData;
import org.streamspinner.connection.CQRowSetListener;
import org.streamspinner.connection.CQRowSetEvent;
import org.streamspinner.connection.CQException;
import org.streamspinner.connection.CQControlEvent;
import org.streamspinner.connection.CQControlEventListener;
import org.streamspinner.connection.DefaultCQRowSet;
import java.util.Vector;
import java.rmi.RemoteException;

public class RemoteStreamServerWrapper extends Wrapper implements CQRowSetListener, CQControlEventListener, Runnable {

	public static String PARAMETER_ATTRIBUTES="attributes";
	public static String PARAMETER_QUERY="query";
	public static String PARAMETER_TABLENAME="tablename";
	public static String PARAMETER_TYPES="types";
	public static String PARAMETER_URL="url";

	private String attributes;
	private String query;
	private String tablename;
	private String types;
	private String url;
	private Schema schema;
	private Thread monitor;
	private boolean connecting;
	private CQRowSet rs;


	public RemoteStreamServerWrapper(String name) throws StreamSpinnerException {
		super(name);
		attributes = null;
		query = null;
		tablename = null;
		types = null;
		url = null;
		schema = null;
		rs = null;
		connecting = false;
	}

	public String[] getAllTableNames(){
		if(tablename != null){
			String[] rval = { tablename };
			return rval;
		}
		else
			return new String[0];
	}

	public Schema getSchema(String name){
		if(tablename.equals(name))
			return schema.copy();
		else
			return null;
	}

	public TupleSet getTupleSet(ORNode on){
		return null;
	}

	public void init() throws StreamSpinnerException {
		query = checkParameter(PARAMETER_QUERY);
		attributes = checkParameter(PARAMETER_ATTRIBUTES);
		types = checkParameter(PARAMETER_TYPES);
		tablename = checkParameter(PARAMETER_TABLENAME);
		url = checkParameter(PARAMETER_URL);

		schema = createSchema();
	}

	private String checkParameter(String parametername) throws StreamSpinnerException {
		String value = getParameter(parametername);
		if(value == null || value.equals(""))
			throw new StreamSpinnerException("Parameter '" + parametername + "' is not specified.");
		return value;
	}

	private Schema createSchema() throws StreamSpinnerException {
		String[] a = attributes.split("\\s*,\\s*");
		String[] t = types.split("\\s*,\\s*");

		if(a == null || a.length == 0 || t == null || t.length == 0 || a.length != t.length)
			throw new StreamSpinnerException("Can not create a schema from given parameters: \"" + attributes + "\", \"" + types + "\"");
		Schema rval = new Schema(tablename, a, t);
		rval.setTableType(Schema.STREAM);
		return rval;
	}

	public void start() throws StreamSpinnerException {
		monitor = new Thread(this);
		monitor.start();
	}

	public void stop() throws StreamSpinnerException {
		try {
			stopConnection();
		} catch(Exception e){
			throw new StreamSpinnerException(e);
		}
		monitor = null;
	}

	private void startConnection() throws CQException {
		if(connecting == false && rs == null){
			rs = new DefaultCQRowSet();
			rs.setUrl(url);
			rs.setCommand(query);
			rs.setAutoAcknowledge(false);
			rs.addCQRowSetListener(this);
			rs.addCQControlEventListener(this);
			rs.start();
		}
		connecting = true;
	}

	private void stopConnection() throws CQException {
		if(connecting == true && rs != null){
			rs.stop();
			rs.removeCQRowSetListener(this);
			rs.removeCQControlEventListener(this);
		}
		rs = null;
		connecting = false;
	}

	public void run() {
		Thread current = Thread.currentThread();
		while(monitor != null && monitor.equals(current) && connecting == false){
			try {
				startConnection();
			} catch(CQException cqe){
				try {
					System.err.println(cqe.getMessage());
					stopConnection();
					Thread.sleep(10000);
				} catch(Exception e){
					e.printStackTrace();
					break;
				}
			}
		}
		if(connecting == true)
			monitor = null;
	}

	private boolean checkSchema(CQRowSetMetaData rsmd) throws CQException {
		if(schema.size() != rsmd.getColumnCount())
			return false;

		for(int i=0; i < schema.size(); i++)
			if(! schema.getType(i).equals(rsmd.getColumnTypeName(i+1)))
				return false;
		return true;
	}

	public void errorOccurred(CQControlEvent cqe){
		System.err.println(cqe.getCQException().getMessage());
		try {
			stopConnection();
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public void dataDistributed(CQRowSetEvent event){
		CQRowSet source = (CQRowSet)(event.getSource());

		Vector tuples = new Vector();
		long now = System.currentTimeMillis();

		try {
			if(checkSchema(source.getMetaData()) == false)
				throw new CQException("schema does not match");
			source.beforeFirst();
			while(source.next()){
				Tuple t = new Tuple(schema.size());
				for(int i=0; i < schema.size(); i++){
					t.setObject(i, source.getObject(i + 1));
				}
				t.setTimestamp(tablename, now);
				tuples.add(t);
			}
			TupleSet ts = new OnMemoryTupleSet(schema, tuples);
			ts.beforeFirst();

			deliverTupleSet(now, tablename, ts);
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public void sendAcknowledge() throws StreamSpinnerException {
		if(connecting == false || rs == null)
			return;
		try {
			rs.acknowledge();
		} catch(CQException cqe){
			throw new StreamSpinnerException(cqe);
		}
	}
}
