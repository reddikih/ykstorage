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
import org.streamspinner.ArrivalTupleListener;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.DataTypes;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.engine.OnMemoryTupleSet;
import org.streamspinner.engine.Tuple;
import org.streamspinner.engine.Schema;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.AttributeList;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.PredicateSet;
import java.util.*;
import java.sql.*;

public class IntervalClockStream extends Wrapper implements Runnable {

	public static String PARAMETER_INTERVAL="interval";
	public static String PARAMETER_SHIFT="shift";

	private Thread monitor;
	private long interval;
	private long shift;
	private String[] table;
	private Schema schema;

	public IntervalClockStream(String name) throws StreamSpinnerException {
		super(name);

		table = new String[1];
		table[0] = name;

		String[] attrs = { name + ".Timestamp" };
		String[] types = { DataTypes.LONG };
		schema = new Schema(table[0], attrs, types);
		schema.setTableType(Schema.STREAM);

		interval = 0;
		shift = 0;
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
				ts.appendTuple(t);
				ts.beforeFirst();

				deliverTupleSet(executiontime, table[0], ts);

				Thread.sleep(interval);
			}
		} catch (Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

}
