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

import org.streamspinner.DataTypes;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.ArrivalTupleListener;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.engine.OnMemoryTupleSet;
import org.streamspinner.engine.Tuple;
import org.streamspinner.engine.Schema;
import org.streamspinner.query.ORNode;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.text.*;

public class CSVFileStream extends Wrapper implements Runnable {

	public static final String PARAMETER_FILENAME = "filename";
	public static final String PARAMETER_TYPE = "type";
	public static final String PARAMETER_TABLENAME = "tablename";
	public static final String PARAMETER_COLUMNNAMES = "columnnames";
	public static final String PARAMETER_COLUMNTYPES = "columntypes";
	public static final String PARAMETER_TIMESTAMP = "timestamp";
	public static final String PARAMETER_OVERWRITETIMESTAMP = "overwritetimestamp";
	public static final String PARAMETER_INTERVAL = "interval";
	public static final String PARAMETER_TUPLESETSIZE = "tuplesetsize";
	public static final String PARAMETER_DATEFORMAT = "dateformat";

	public static final String TYPE_PERIODIC = "periodic";
	public static final String TYPE_DATATIME = "datatime";

	public static final long DEFAULT_INTERVAL = 5000;
	public static final int DEFAULT_TUPLESETSIZE = 10;

	private static Pattern pattern = Pattern.compile("\\s*\"([^\"]*?)\"\\s*,?");

	private String filename = null;
	private String type = null;
	private String tablename = null;
	private String timestamp = null;
	private long interval = DEFAULT_INTERVAL;
	private int tuplesetsize = DEFAULT_TUPLESETSIZE;
	private boolean overwritetimestamp = false;
	private SimpleDateFormat dateformat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);

	private Thread monitor;
	private Schema schema;
	private BufferedReader br;

	public CSVFileStream(String name) throws StreamSpinnerException {
		super(name);
		monitor = null;
		schema = null;
		br = null;
	}

	public String[] getAllTableNames(){
		String[] rval = { tablename };
		return rval;
	}

	public Schema getSchema(String name){
		if(! tablename.equals(name))
			return null;
		else
			return schema;
	}

	public TupleSet getTupleSet(ORNode node) throws StreamSpinnerException {
		return null;
	}

	public void init() throws StreamSpinnerException {
		filename = getParameter(PARAMETER_FILENAME);
		type = getParameter(PARAMETER_TYPE);
		tablename = getParameter(PARAMETER_TABLENAME);
		if(filename == null || type == null || tablename == null)
			throw new StreamSpinnerException("'filename' and 'type' and 'tablename' parameters are required");

		File f = new File(filename);
		if(! f.canRead())
			throw new StreamSpinnerException("file " + filename + " is not readable");

		createSchema();

		try {
			if(getParameter(PARAMETER_INTERVAL) != null)
				interval = Long.parseLong(getParameter(PARAMETER_INTERVAL));
			if(interval <= 0)
				interval = DEFAULT_INTERVAL;
			if(getParameter(PARAMETER_TUPLESETSIZE) != null)
				tuplesetsize = Integer.parseInt(getParameter(PARAMETER_TUPLESETSIZE));
			if(tuplesetsize <= 0)
				tuplesetsize = DEFAULT_TUPLESETSIZE;
		} catch(NumberFormatException nfe){
			throw new StreamSpinnerException(nfe);
		}

		if(type.equals(TYPE_PERIODIC)){
			;
		}
		else if(type.equals(TYPE_DATATIME)){
			timestamp = getParameter(PARAMETER_TIMESTAMP);
			if(timestamp == null)
				throw new StreamSpinnerException("'timestamp' parameter is required for DATATIME mode");
			if(! timestamp.startsWith(tablename + "."))
				timestamp = tablename + "." + timestamp;
			try {
				String t = schema.getType(timestamp);
				if((! t.equals(DataTypes.LONG)) && (! t.equals(DataTypes.STRING)))
					throw new StreamSpinnerException("'timestamp' attribute must be LONG or STRING");
				if(t.equals(DataTypes.STRING) && getParameter(PARAMETER_DATEFORMAT) != null)
					dateformat = new SimpleDateFormat(getParameter(PARAMETER_DATEFORMAT), Locale.ENGLISH);
				if(getParameter(PARAMETER_OVERWRITETIMESTAMP) != null)
					overwritetimestamp = new Boolean(getParameter(PARAMETER_OVERWRITETIMESTAMP));
			} catch(IllegalArgumentException iae){
				throw new StreamSpinnerException(iae);
			}
		}
	}

	private void createSchema() throws StreamSpinnerException {
		if(getParameter(PARAMETER_COLUMNNAMES) == null || getParameter(PARAMETER_COLUMNTYPES) == null)
			throw new StreamSpinnerException("'columnnames' and 'columntypes' parameters are required");
		String[] columnnames = getParameter(PARAMETER_COLUMNNAMES).split("\\s*,\\s*");
		String[] columntypes = getParameter(PARAMETER_COLUMNTYPES).split("\\s*,\\s*");
		if(columnnames.length != columntypes.length)
			throw new StreamSpinnerException("Invalid length of columnnames and columntypes");

		for(int i=0; i < columnnames.length; i++){
			if(! columnnames[i].startsWith(tablename + "."))
				columnnames[i] = tablename + "." + columnnames[i];
		}

		schema = new Schema(tablename, columnnames, columntypes);
		schema.setTableType(Schema.STREAM);
	}

	public void start() throws StreamSpinnerException {
		if(br == null){
			try {
				br = new BufferedReader(new FileReader(filename));
			} catch(IOException ioe){
				throw new StreamSpinnerException(ioe);
			}
		}
		if(monitor == null){
			monitor = new Thread(this);
			monitor.start();
		}
	}

	public void stop() throws StreamSpinnerException {
		if(monitor != null)
			monitor = null;
		if(br != null){
			try {
				br.close();
				br = null;
			} catch(IOException ioe){
				throw new StreamSpinnerException(ioe);
			}
		}
	}

	public void run(){
		try {
			Thread ct = Thread.currentThread();
			long sleepinterval = interval;
			Tuple lasttuple = null;

			Thread.sleep(sleepinterval);

			String line = null;
			if(br != null)
				line = br.readLine();

			while(monitor != null && monitor.equals(ct) && br != null && line != null){
				long executiontime = System.currentTimeMillis();
				OnMemoryTupleSet ts = new OnMemoryTupleSet(schema);

				if(type.equals(TYPE_PERIODIC)){
					for(int i=0; i < tuplesetsize && line != null; i++){
						Tuple t = parse(line);
						t.setTimestamp(tablename, executiontime);
						ts.appendTuple(t);
						line = br.readLine();
					}
				}
				else if(type.equals(TYPE_DATATIME)){
					if(lasttuple == null)
						lasttuple = parse(line);
					long begintime = getDataTimestamp(lasttuple);
					long endtime = begintime;
					while(lasttuple != null && begintime == endtime){
						lasttuple.setTimestamp(tablename, executiontime);
						if(overwritetimestamp == true)
							overwriteTimestamp(lasttuple);
						ts.appendTuple(lasttuple);
						line = br.readLine();
						lasttuple = parse(line);
						endtime = getDataTimestamp(lasttuple);
					}
					sleepinterval = endtime - begintime;
				}

				deliverTupleSet(executiontime, tablename, ts);

				if(line == null)
					break;
				Thread.sleep(sleepinterval);
			}
		} catch(Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	private Tuple parse(String line) throws Exception {
		if(line == null)
			return null;

		ArrayList values = new ArrayList();
		Matcher m = pattern.matcher(line);
		while(m.find())
			values.add(m.group(1));

		Tuple tuple = new Tuple(schema.size());
		for(int i=0; i < schema.size() && i < values.size(); i++){
			String t = schema.getType(i);
			if(t.equals(DataTypes.LONG))
				tuple.setLong(i, Long.parseLong(values.get(i).toString()));
			else if(t.equals(DataTypes.DOUBLE))
				tuple.setDouble(i, Double.parseDouble(values.get(i).toString()));
			else if(t.equals(DataTypes.STRING))
				tuple.setString(i, values.get(i).toString());
			else if(t.equals(DataTypes.OBJECT))
				tuple.setObject(i, values.get(i));
			else
				throw new StreamSpinnerException("Unknown data type: " + type);
		}
		return tuple;
	}

	private long getDataTimestamp(Tuple tuple) throws StreamSpinnerException {
		if(tuple == null)
			return 0;
		String t = schema.getType(timestamp);
		int index = schema.getIndex(timestamp);
		if(t.equals(DataTypes.LONG))
			return tuple.getLong(index);
		else if(t.equals(DataTypes.STRING)){
			Date d = dateformat.parse(tuple.getString(index), new ParsePosition(0));
			return d.getTime();
		}
		throw new StreamSpinnerException("this datatype is not supported : " + t);
	}

	private void overwriteTimestamp(Tuple tuple) throws StreamSpinnerException {
		if(overwritetimestamp == false)
			return;
		long ts = tuple.getTimestamp(tablename);
		int index = schema.getIndex(timestamp);
		String t = schema.getType(timestamp);
		if(t.equals(DataTypes.LONG))
			tuple.setLong(index, ts);
		else if(t.equals(DataTypes.STRING))
			tuple.setString(index, (new Date(ts)).toString());
		else
			throw new StreamSpinnerException("this datatype is not supported : " + t);
	}

}
