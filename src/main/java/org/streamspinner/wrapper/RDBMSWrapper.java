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
import org.streamspinner.engine.JDBCTupleSet;
import org.streamspinner.engine.Schema;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.AttributeList;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.PredicateSet;
import java.util.*;
import java.sql.*;

public class RDBMSWrapper extends Wrapper {

	public static String PROPERTY_DRIVER = "driver";
	public static String PROPERTY_URL = "url";
	public static String PROPERTY_USER = "user";
	public static String PROPERTY_PASSWORD = "password";

	private String url;
	private String user;
	private String password;
	private String driver;

	private Connection conn;
	private String[] tablenames;
	private HashMap schemas;


	public RDBMSWrapper(String name) throws StreamSpinnerException {
		super(name);

		tablenames = null;
		schemas = null;
		conn = null;
		driver = null;
		url = null;
		user = null;
		password = null;
	}

	public String[] getAllTableNames(){
		return tablenames;
	}

	public Schema getSchema(String tablename){
		if(schemas.containsKey(tablename))
			return (Schema)(schemas.get(tablename));
		else
			return null;
	}

	public TupleSet getTupleSet(ORNode node) throws StreamSpinnerException {
		if(conn == null)
			throw new StreamSpinnerException("RDBMS Wrapper is not started");

		StringBuffer sql = new StringBuffer("");

		sql.append("SELECT ");
		sql.append(node.getAttributeList().toString());

		sql.append("  FROM ");
		SourceSet sources = node.getSources();
		Iterator sit = sources.iterator();
		while(sit.hasNext()){
			sql.append(sit.next());
			if(sit.hasNext())
				sql.append(", ");
		}

		PredicateSet conds = node.getConditions();
		if(conds.size() > 0){
			sql.append("  WHERE ");
			sql.append(conds.toString());
		}

		try {
			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet rs = stmt.executeQuery(sql.toString());

			TupleSet ts = new JDBCTupleSet(rs);
			return ts;
		} 
		catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public void init() throws StreamSpinnerException {
		driver = getParameter(PROPERTY_DRIVER);
		url = getParameter(PROPERTY_URL);
		user = getParameter(PROPERTY_USER);
		password = getParameter(PROPERTY_PASSWORD);

		try {
			Class.forName(driver);

			if(conn == null)
				conn = DriverManager.getConnection(url, user, password);

			createSchemas();

			conn.close();
			conn = null;
		} catch(ClassNotFoundException ce){
			throw new StreamSpinnerException(ce);
		} catch (SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public void start() throws StreamSpinnerException {
		try {
			Class.forName(driver);

			if(conn == null)
				conn = DriverManager.getConnection(url, user, password);

		} catch(ClassNotFoundException ce){
			throw new StreamSpinnerException(ce);
		} catch (SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	public void stop() throws StreamSpinnerException {
		try {
			if(conn != null){
				conn.close();
				conn = null;
			}
		} catch(SQLException e){
			throw new StreamSpinnerException(e);
		}
	}

	private void createSchemas() throws SQLException, StreamSpinnerException {
		DatabaseMetaData dbmd = conn.getMetaData();

		tablenames = getTableNames(dbmd);
		schemas = new HashMap();

		for(int i=0; i < tablenames.length; i++){
			String[][] columns = getTableColumns(dbmd, tablenames[i]);
			Schema schema = new Schema(tablenames[i], columns[0], columns[1]);
			schema.setTableType(Schema.RDB);
			schemas.put(tablenames[i], schema);
		}
	}


	private String[] getTableNames(DatabaseMetaData dbmd) throws SQLException, StreamSpinnerException {
		ArrayList names = new ArrayList();

		String[] tabletypes = { "TABLE", "VIEW" };
		ResultSet rs = dbmd.getTables(null, null, null, tabletypes);

		while(rs.next()){
			names.add(rs.getString(3));
		}
		rs.close();

		String[] rval = new String[names.size()];
		rval = (String[])(names.toArray(rval));

		return rval;
	}


	private String[][] getTableColumns(DatabaseMetaData dbmd, String table) throws SQLException, StreamSpinnerException {
		ArrayList cnames = new ArrayList();
		ArrayList ctypes = new ArrayList();

		ResultSet rs = dbmd.getColumns(null, null, table, null);
		while(rs.next()){
			cnames.add(table + "." + rs.getString(4));
			ctypes.add(DataTypes.convertSQLType(rs.getInt(5)));
		}
		rs.close();

		String[] rval0 = new String[cnames.size()];
		rval0 = (String[])(cnames.toArray(rval0));

		String[] rval1 = new String[ctypes.size()];
		rval1 = (String[])(ctypes.toArray(rval1));

		String[][] rval = { rval0, rval1 };
		return rval;
	}

}

