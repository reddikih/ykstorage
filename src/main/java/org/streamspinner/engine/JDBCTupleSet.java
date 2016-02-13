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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class JDBCTupleSet implements TupleSet {

	private Schema schema;
	private ResultSet rs;

	public JDBCTupleSet(ResultSet rs) throws StreamSpinnerException {
		try {
			this.rs = rs;
			this.schema = new Schema(rs.getMetaData());
		} catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public Schema getSchema() throws StreamSpinnerException {
		return schema;
	}

	public void afterLast() throws StreamSpinnerException {
		try {
			rs.afterLast();
		} catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public void beforeFirst() throws StreamSpinnerException {
		try {
			rs.beforeFirst();
		} catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public void close() throws StreamSpinnerException {
		try{
			rs.close();
		} catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public boolean first() throws StreamSpinnerException {
		try {
			return rs.first();
		} catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public Tuple getTuple() throws StreamSpinnerException {
		try{
			Tuple t = new Tuple(schema.size());

			for(int i=0; i < schema.size(); i++){
				String type = schema.getType(i);
				if(type.equals(DataTypes.LONG))
					t.setLong(i, rs.getLong(i+1));
				else if(type.equals(DataTypes.DOUBLE))
					t.setDouble(i, rs.getDouble(i+1));
				else if(type.equals(DataTypes.STRING))
					t.setString(i, rs.getString(i+1));
				else
					t.setObject(i, rs.getObject(i+1));
			}
			return t;
		} catch (SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public boolean last() throws StreamSpinnerException  {
		try {
			return rs.last();
		} catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public boolean next() throws StreamSpinnerException  {
		try {
			return rs.next();
		} catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public boolean previous() throws StreamSpinnerException {
		try {
			return rs.previous();
		} catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

}


