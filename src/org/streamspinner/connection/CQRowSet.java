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
package org.streamspinner.connection;

public interface CQRowSet {

	public void addCQRowSetListener(CQRowSetListener listener);

	public void removeCQRowSetListener(CQRowSetListener listener);

	public void addCQControlEventListener(CQControlEventListener listener);

	public void removeCQControlEventListener(CQControlEventListener listener);

	public void setCommand(String cmd);

	public String getCommand();

	public void setUrl(String url);

	public String getUrl();

	public void useVariableInetAddress(boolean flag);

	public void execute() throws CQException;

	public void start() throws CQException;

	public void stop() throws CQException;

	public void beforeFirst() throws CQException;

	public void afterLast() throws CQException;

	public boolean first() throws CQException;

	public boolean last() throws CQException;

	public boolean absolute(int row) throws CQException;

	public boolean relative(int rows) throws CQException;

	public boolean next() throws CQException;

	public boolean previous() throws CQException;

	public int findColumn(String columnName) throws CQException;

	public boolean isEmpty() throws CQException;

	public CQRowSetMetaData getMetaData() throws CQException;

	public double getDouble(int columnIndex) throws CQException;

	public double getDouble(String columnName) throws CQException;

	public long getLong(int columnIndex) throws CQException;

	public long getLong(String columnName) throws CQException;

	public Object getObject(int columnIndex) throws CQException;

	public Object getObject(String columnName) throws CQException;

	public String getString(int columnIndex) throws CQException;

	public String getString(String columnName) throws CQException;

	public boolean isBeforeFirst() throws CQException;

	public boolean isFirst() throws CQException;

	public boolean isLast() throws CQException;

	public boolean isAfterLast() throws CQException;

	public void setAutoAcknowledge(boolean flag);

	public boolean getAutoAcknowledge();

	public void acknowledge() throws CQException ;

}
