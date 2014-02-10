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

public class RandomStreamGenerator2 extends Wrapper implements Runnable {

	public static String PARAMETER_INTERVAL="interval";
	public static String PARAMETER_SHIFT="shift";
	public static String PARAMETER_TABLENAME="tablename";
	public static String PARAMETER_TYPE="type";
	public static String PARAMETER_COLUMNS="columns";
	public static String PARAMETER_ROWS="rows";

	private Thread monitor;
	private long interval;
	private long shift;
	private long rows;
	private long columns;
	private String type;
	private String tablename;
	private Schema schema;
	private Random rand;

	public RandomStreamGenerator2(String name) throws StreamSpinnerException {
		super(name);

		monitor = null;
		tablename = null;
		type = null;
		schema = null;
		interval = 0;
		shift = 0;
		rows = 0;
		columns = 0;
		rand = new Random();
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
		if(interval <= 0)
			throw new StreamSpinnerException("Parameter 'interval' is invalid: " + interval);
		shift = Long.parseLong(getParameter(PARAMETER_SHIFT));
		if(shift <= 0)
			throw new StreamSpinnerException("Parameter 'shift' is invalid: " + shift);
		rows = Long.parseLong(getParameter(PARAMETER_ROWS));
		if(rows <= 0)
			throw new StreamSpinnerException("Parameter 'rows' is invalid: " + rows);
		columns = Long.parseLong(getParameter(PARAMETER_COLUMNS));
		if(columns <= 0)
			throw new StreamSpinnerException("Parameter 'columns' is invalid: " + columns);
		type = checkParameter(PARAMETER_TYPE);
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
		t.add(DataTypes.LONG);
		for(int i=0; i < columns; i++)
			t.add(type);

		List<String> a = new ArrayList<String>();
		a.add(tablename + ".Timestamp");
		a.add(tablename + ".Class");
		for(int i=0; i < columns; i++)
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

			long deliverytime = System.currentTimeMillis() + shift;
			OnMemoryTupleSet ts = createTupleSet(deliverytime);

			while(monitor != null && monitor.equals(ct)){
				Thread.sleep(deliverytime - System.currentTimeMillis());
				deliverTupleSet(deliverytime, tablename, ts);

				deliverytime = Math.max(deliverytime + interval, System.currentTimeMillis());
				ts = createTupleSet(deliverytime);
			}
		} catch (Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	private OnMemoryTupleSet createTupleSet(long time) throws StreamSpinnerException {
		OnMemoryTupleSet ts = new OnMemoryTupleSet(schema);
		for(int i=0; i < rows; i++)
			ts.appendTuple(createTuple(i, time));
		ts.beforeFirst();
		return ts;
	}

	private Tuple createTuple(long num, long time) throws StreamSpinnerException {
		Tuple tuple = new Tuple(schema.size());

		tuple.setTimestamp(tablename, time);
		tuple.setLong(0, time);
		tuple.setLong(1, num % 10);

		for(int i=2; i < schema.size(); i++){
			int tmp = rand.nextInt();
			if(type.equals(DataTypes.STRING)){
				tuple.setString(i, "string" + tmp);
			}
			else if(type.equals(DataTypes.LONG)){
				tuple.setLong(i, (long)tmp);
			}
			else if(type.equals(DataTypes.DOUBLE)){
				tuple.setDouble(i, (double)tmp);
			}
			else if(type.equals(DataTypes.ARRAY_STRING)){
				String[] array = new String[tmp];
				for(int j=0; j < array.length; j++)
					array[j] = "string" + rand.nextInt(10);
				tuple.setObject(i, array);
			}
			else if(type.equals(DataTypes.ARRAY_LONG)){
				long[] array = new long[tmp];
				for(int j=0; j < array.length; j++)
					array[j] = (long)(rand.nextInt(10));
				tuple.setObject(i, array);
			}
			else if(type.equals(DataTypes.ARRAY_DOUBLE)){
				double[] array = new double[tmp];
				for(int j=0; j < array.length; j++)
					array[j] = (double)(rand.nextInt(10));
				tuple.setObject(i, array);
			}
			else if(type.equals(DataTypes.ARRAY_OBJECT)){
				Object[] array = new Object[tmp];
				for(int j=0; j < array.length; j++)
					array[j] = new Object();
				tuple.setObject(i, array);
			}
			else
				tuple.setObject(i, new Object());
		}
		return tuple;
	}

}
