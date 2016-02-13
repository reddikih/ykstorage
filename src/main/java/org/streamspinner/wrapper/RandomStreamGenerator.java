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
import java.util.*;
import java.sql.*;

public class RandomStreamGenerator extends Wrapper implements Runnable {

	public static String PARAMETER_INTERVAL="interval";
	public static String PARAMETER_SHIFT="shift";
	public static String PARAMETER_TABLENAME="tablename";
	public static String PARAMETER_TYPES="types";

	private Thread monitor;
	private long interval;
	private long shift;
	private String types;
	private String tablename;
	private Schema schema;

	public RandomStreamGenerator(String name) throws StreamSpinnerException {
		super(name);

		monitor = null;
		tablename = null;
		types = null;
		schema = null;
		interval = 0;
		shift = 0;
	}

	public String[] getAllTableNames(){
		if(tablename == null)
			return new String[0];
		else {
			String[] rval = { tablename };
			return rval;
		}
	}

	public Schema getSchema(String tname){
		if(! tablename.equals(tname))
			return null;
		return schema;
	}

	public TupleSet getTupleSet(ORNode node) throws StreamSpinnerException {
		return null;
	}

	public void init() throws StreamSpinnerException {
		interval = Long.parseLong(getParameter(PARAMETER_INTERVAL));
		shift = Long.parseLong(getParameter(PARAMETER_SHIFT));
		types = checkParameter(PARAMETER_TYPES);
		tablename = checkParameter(PARAMETER_TABLENAME);

		createSchema();
	}

	private String checkParameter(String parametername) throws StreamSpinnerException {
		String value = getParameter(parametername);
		if(value == null || value.equals(""))
			throw new StreamSpinnerException("Parameter '" + parametername + "' is not specified.");
		return value;
	}

	private void createSchema() throws StreamSpinnerException {
		List<String> t = new ArrayList<String>();
		t.add(DataTypes.LONG);
		t.addAll(Arrays.asList(types.split("\\s*,\\s*")));

		List<String> a = new ArrayList<String>();
		a.add(tablename + ".Timestamp");
		for(int i=1; i < t.size(); i++)
			a.add(tablename + ".A" + i);

		schema = new Schema(tablename, (String[])(a.toArray(new String[0])), (String[])(t.toArray(new String[0])));
		schema.setTableType(Schema.STREAM);
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
				Tuple t = createTuple(executiontime);
				ts.appendTuple(t);
				ts.beforeFirst();

				deliverTupleSet(executiontime, tablename, ts);

				Thread.sleep(interval);
			}
		} catch (Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	private Tuple createTuple(long time) throws StreamSpinnerException {
		Tuple t = new Tuple(schema.size());
		Random r = new Random(time);

		t.setTimestamp(tablename, time);
		t.setLong(0, time);

		//int tmp = 1 + r.nextInt(10);
		int tmp = 1 + r.nextInt(15);	// randomの値を大きく変更

		for(int i=1; i < schema.size(); i++){
			String type = schema.getType(i);

			if(type.equals(DataTypes.STRING)){
				t.setString(i, "string" + tmp);
			}
			else if(type.equals(DataTypes.LONG)){
				t.setLong(i, (long)tmp);
			}
			else if(type.equals(DataTypes.DOUBLE)){
				t.setDouble(i, (double)tmp);
			}
			else if(type.equals(DataTypes.ARRAY_STRING)){
				String[] array = new String[tmp];
				for(int j=0; j < array.length; j++)
					array[j] = "string" + r.nextInt(10);
				t.setObject(i, array);
			}
			else if(type.equals(DataTypes.ARRAY_LONG)){
				long[] array = new long[tmp];
				for(int j=0; j < array.length; j++)
					array[j] = (long)(r.nextInt(10));
				t.setObject(i, array);
			}
			else if(type.equals(DataTypes.ARRAY_DOUBLE)){
				double[] array = new double[tmp];
				for(int j=0; j < array.length; j++)
					array[j] = (double)(r.nextInt(10));
				t.setObject(i, array);
			}
			else if(type.equals(DataTypes.ARRAY_OBJECT)){
				Object[] array = new Object[tmp];
				for(int j=0; j < array.length; j++)
					array[j] = new Object();
				t.setObject(i, array);
			}
			else
				t.setObject(i, new Object());
		}
		return t;
	}

}
