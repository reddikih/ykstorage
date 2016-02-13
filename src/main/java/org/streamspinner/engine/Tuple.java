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
import org.streamspinner.query.SourceSet;
import java.io.Serializable;
import java.util.Date;
import java.util.Arrays;
import java.util.Iterator;

public class Tuple implements Serializable {

	private Object[] values;

	private String[] sourcenames;
	private long[] timestamps;

	public Tuple(int size){
		values = new Object[size];

		sourcenames = new String[0];
		timestamps = new long[0];
	}


	public Tuple(Tuple t1, Tuple t2) {
		values = new Object[t1.size() + t2.size()];
		for(int i=0; i < values.length; i++){
			if(i < t1.values.length)
				values[i] = t1.values[i];
			else
				values[i] = t2.values[i - t1.values.length];
		}

		sourcenames = new String[0];
		timestamps = new long[0];
		setTimestamps(t1.sourcenames, t1.timestamps);
		setTimestamps(t2.sourcenames, t2.timestamps);
	}

	public Tuple appendColumn(){
		Tuple rval = new Tuple(values.length + 1);
		System.arraycopy(values, 0, rval.values, 0, values.length);
		rval.values[rval.values.length -1] = null;
		rval.setTimestamps(sourcenames, timestamps);
		return rval;
	}

	public Tuple subset(int[] indexes) throws StreamSpinnerException {
		try {
			Tuple rval = new Tuple(indexes.length);
			for(int i=0; i < indexes.length; i++)
				rval.values[i] = values[indexes[i]];
			rval.setTimestamps(sourcenames, timestamps);
			return rval;
		} catch(ArrayIndexOutOfBoundsException e){
			throw new StreamSpinnerException(e);
		}
	}

	public void setTimestamp(String source, long t){
		String[] tmpsrc = new String[sourcenames.length + 1];
		long[] tmpts = new long[timestamps.length + 1];

		System.arraycopy(sourcenames, 0, tmpsrc, 0, sourcenames.length);
		System.arraycopy(timestamps, 0, tmpts, 0, timestamps.length);
		tmpsrc[tmpsrc.length -1] = source;
		tmpts[tmpts.length -1] = t;

		sourcenames = tmpsrc;
		timestamps = tmpts;
	}

	public void setTimestamps(String[] sources, long[] times){
		String[] tmpsrc = new String[sourcenames.length + sources.length];
		long[] tmpts = new long[timestamps.length + times.length];

		System.arraycopy(sourcenames, 0, tmpsrc, 0, sourcenames.length);
		System.arraycopy(sources, 0, tmpsrc, sourcenames.length, sources.length);

		System.arraycopy(timestamps, 0, tmpts, 0, timestamps.length);
		System.arraycopy(times, 0, tmpts, timestamps.length, times.length);

		sourcenames = tmpsrc;
		timestamps = tmpts;
	}

	public void setLong(int index, long value) throws StreamSpinnerException {
		checkIndex(index);
		values[index] = new Long(value);
	}

	public void setDouble(int index, double value) throws StreamSpinnerException {
		checkIndex(index);
		values[index] = new Double(value);
	}

	public void setString(int index, String value) throws StreamSpinnerException {
		checkIndex(index);
		values[index] = new String(value);
	}

	public void setObject(int index, Object value) throws StreamSpinnerException {
		checkIndex(index);
		values[index] = value;
	}


	public long getTimestamp(String source){
		for(int i=0; i < sourcenames.length && i < timestamps.length; i++){
			if(sourcenames[i].equals(source))
				return timestamps[i];
		}
		return Long.MIN_VALUE;
	}

	public long getMaxTimestamp(){
		long max = Long.MIN_VALUE;
		for(int i=0; i < timestamps.length; i++){
			if(max < timestamps[i])
				max = timestamps[i];
		}
		return max;
	}

	public long getMinTimestamp(){
		long min = Long.MAX_VALUE;
		for(int i=0; i < timestamps.length; i++){
			if(min > timestamps[i])
				min = timestamps[i];
		}
		if(min == Long.MAX_VALUE)
			return Long.MIN_VALUE;
		else
			return min;
	}

	public boolean isIncludedWindow(long now, SourceSet sources){
		if(now == Long.MIN_VALUE)
			return false;
		for(int i=0; i < sourcenames.length && i < timestamps.length; i++){
			if(! sources.contains(sourcenames[i]))
				continue;
			long window = sources.getWindowsize(sourcenames[i]);
			long origin = sources.getWindowOrigin(sourcenames[i]);
			if(window == Long.MAX_VALUE){
				if(timestamps[i] <= now)
					continue;
				else if(timestamps[i] > now)
					return false;
			}
			if(origin <= 0 && (timestamps[i] > now + origin || now + origin - window > timestamps[i]) )
				return false;
			else if(origin > 0 && (timestamps[i] > origin || origin - window > timestamps[i]) )
				return false;
		}
		return true;
	}

	public boolean isNewerThan(long time){
		for(int i=0; i < timestamps.length; i++)
			if(timestamps[i] <= time)
				return false;
		return true;
	}

	public boolean isOlderThan(long time){
		for(int i=0; i < timestamps.length; i++)
			if(timestamps[i] >= time)
				return false;
		return true;
	}

	public boolean willBeIncludedWindow(long now, SourceSet sources){
		for(int i=0; i < sourcenames.length && i < timestamps.length; i++){
			if(! sources.contains(sourcenames[i]))
				continue;
			long window = sources.getWindowsize(sourcenames[i]);
			long origin = sources.getWindowOrigin(sourcenames[i]);
			if(window == Long.MAX_VALUE)
				continue;
			if(origin <= 0 && timestamps[i] < now + origin - window)
				return false;
			else if(origin > 0 && (! isIncludedWindow(now, sources) ) )
				return false;
		}
		return true;
	}

	public long getLong(int index) throws StreamSpinnerException {
		checkIndex(index);
		try {
			if(values[index] != null)
				return ((Long)(values[index])).longValue();
		} catch (ClassCastException e){
			throw new StreamSpinnerException(e);
		}
		return 0;
	}


	public double getDouble(int index) throws StreamSpinnerException {
		checkIndex(index);
		try {
			if(values[index] != null)
				return ((Double)(values[index])).doubleValue();
		} catch(ClassCastException e){
			throw new StreamSpinnerException(e);
		}
		return 0.0;
	}


	public String getString(int index) throws StreamSpinnerException {
		checkIndex(index);
		try {
			if(values[index] != null)
				return DataTypes.toString(values[index]);
		} catch(ClassCastException e){
			throw new StreamSpinnerException(e);
		}
		return new String();
	}

	public Object getObject(int index) throws StreamSpinnerException {
		checkIndex(index);
		return values[index];
	}

	private boolean checkIndex(int index) throws StreamSpinnerException {
		if(index < 0 || index >= values.length)
			throw new StreamSpinnerException("index is out of bound");
		return true;
	}

	public String toString(){
		StringBuffer sbuf = new StringBuffer();

		sbuf.append("<tuple>\n");
		for(int i=0; i < sourcenames.length && i < timestamps.length; i++){
			Date d = new Date(timestamps[i]);
			sbuf.append("  <timestamp source=\"" + sourcenames[i] + "\" value=\"" + d.toString() + "\" />\n");
		}
		for(int i=0; i < values.length; i++){
			sbuf.append("  <column>");
			if(values[i] instanceof Object[])
				sbuf.append(Arrays.toString((Object[])values[i]));
			else
				sbuf.append(values[i].toString());
			sbuf.append("</column>\n");
		}
		sbuf.append("</tuple>");
		return sbuf.toString();
	}


	public Tuple copy(){
		Tuple rval = new Tuple(size());
		System.arraycopy(values, 0, rval.values, 0, values.length);
		rval.setTimestamps(sourcenames, timestamps);
		return rval;
	}


	public int size(){
		return values.length;
	}


	public static Tuple[] copyTuples(Tuple[] tset) {
		Tuple[] rval = new Tuple[tset.length];
		for(int i=0; i < rval.length; i++)
			rval[i] = tset[i].copy();
		return rval;
	}


	public boolean equals(Object o){
		if(! (o instanceof org.streamspinner.engine.Tuple))
			return false;
		Tuple target = (Tuple)o;
		if(Arrays.deepEquals(values, target.values) == false)
			return false;
		for(int i=0; i < sourcenames.length; i++){
			boolean found = false;
			for(int j=0; j < target.sourcenames.length; j++){
				if(sourcenames[i].equals(target.sourcenames[j])){
					found = true;
					if(timestamps[i] != target.timestamps[j])
						return false;
				}
			}
			if(found == false)
				return false;
		}
		return true;
	}

}
