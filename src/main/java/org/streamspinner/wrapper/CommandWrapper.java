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

import org.streamspinner.*;
import org.streamspinner.engine.*;
import org.streamspinner.query.*;
import java.io.*;

public class CommandWrapper extends Wrapper implements Runnable {

	public static String PARAMETER_INTERVAL="interval";
	public static String PARAMETER_SHIFT="shift";
	public static String PARAMETER_COMMAND="command";

	private Thread monitor = null;
	private long interval = 0;
	private long shift = 0;
	private String command = null;
	private String[] table = null;
	private Schema schema = null;

	public CommandWrapper(String name) throws StreamSpinnerException {
		super(name);

		table = new String[1];
		table[0] = "Command";

		String[] attrs = { table[0]+".Timestamp", table[0]+".Name", table[0]+".Output" };
		String[] types = { DataTypes.LONG, DataTypes.STRING, DataTypes.STRING };
		schema = new Schema(table[0], attrs, types);
		schema.setTableType(Schema.STREAM);
	}

	public String[] getAllTableNames(){
		return table;
	}

	public Schema getSchema(String tablename){
		if(! tablename.equals(table[0]))
			return null;
		return schema;
	}

	public TupleSet getTupleSet(ORNode node) throws StreamSpinnerException {
		return null;
	}

	public void init() throws StreamSpinnerException {
		interval = Long.parseLong(getParameter(PARAMETER_INTERVAL));
		shift = Long.parseLong(getParameter(PARAMETER_SHIFT));
		command = getParameter(PARAMETER_COMMAND);
	}

	public void start() throws StreamSpinnerException {
		if(interval <= 0)
			throw new StreamSpinnerException("interval is not set");
		monitor = new Thread(this);
		monitor.start();
	}

	public void stop(){
		monitor = null;
	}

	public void run(){
		try{
			Thread ct = Thread.currentThread();
			Thread.sleep(shift);

			while(monitor != null && monitor.equals(ct)){
				long executiontime = System.currentTimeMillis();

				OnMemoryTupleSet ts = new OnMemoryTupleSet(schema);
				Tuple t = new Tuple(schema.size());
				t.setTimestamp(table[0], executiontime);
				t.setLong(0, executiontime);
				t.setString(1, command);
				t.setString(2, getCommandOutput());
				ts.appendTuple(t);
				ts.beforeFirst();

				deliverTupleSet(executiontime, table[0], ts);

				Thread.sleep(interval);
			}
		} catch (Exception e){
			e.printStackTrace();
		}
	}


	public String getCommandOutput() throws StreamSpinnerException {
		try {
			Runtime r = Runtime.getRuntime();
			Process p = r.exec(command);
			InputStreamReader isr = new InputStreamReader(p.getInputStream());
			BufferedReader br = new BufferedReader(isr);
			StringBuilder rval = new StringBuilder();
			String line = null;
			while((line = br.readLine()) != null)
				rval.append(line);
			br.close();
			return rval.toString();
		} catch(Exception e){
			throw new StreamSpinnerException(e);
		}
	}
}
