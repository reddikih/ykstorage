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
import java.util.List;
import java.util.ArrayList;

public class OnMemoryTupleSet implements TupleSet {

	private Schema schema;
	private List list;
	private int index;

	public OnMemoryTupleSet(Schema s){
		schema = s;
		list = new ArrayList();
		index = -1;
	}

	public OnMemoryTupleSet(Schema s, List l){
		this(s);
		list.addAll(l);
	}

	public Schema getSchema() throws StreamSpinnerException {
		return schema;
	}

	public void afterLast() throws StreamSpinnerException {
		index = list.size();
	}

	public void beforeFirst() throws StreamSpinnerException {
		index = -1;
	}

	public void close() throws StreamSpinnerException {
		schema = null;
		list = null;
	}

	public boolean first() throws StreamSpinnerException {
		index = Math.min(0, list.size() -1);
		return index >= 0 && index < list.size();
	}

	public Tuple getTuple() throws StreamSpinnerException {
		if(index >= 0 && index < list.size())
			return (Tuple)list.get(index);
		else
			throw new StreamSpinnerException("Array index is out of bound");
	}

	public boolean last() throws StreamSpinnerException {
		index = list.size() -1;
		return index >= 0 && index < list.size();
	}

	public boolean next() throws StreamSpinnerException {
		int n = index + 1;
		if( n >= 0 && n < list.size() ){
			index++;
			return true;
		}
		else 
			return false;
	}

	public boolean previous() throws StreamSpinnerException {
		int n = index -1;
		if( n >= 0 && n < list.size() ){
			index--;
			return true;
		}
		else
			return false;
	}

	protected void append(Tuple t) throws StreamSpinnerException {
		index++;
		list.add(index, t);
	}

	public void appendTuple(Tuple t) throws StreamSpinnerException {
		validateTypes(t);
		append(t);
	}

	private void validateTypes(Tuple t) throws StreamSpinnerException {
		if(t.size() != schema.size())
			throw new StreamSpinnerException("Size of tuple is not valid");
		/*
		for(int i=0; i < schema.size(); i++){
			String typename = schema.getType(i);
			if(typename.equals(DataTypes.LONG))
				t.getLong(i);
			else if(typename.equals(DataTypes.DOUBLE))
				t.getDouble(i);
			else if(typename.equals(DataTypes.STRING))
				t.getString(i);
			else
				t.getObject(i);
		}
		*/
	}

}

