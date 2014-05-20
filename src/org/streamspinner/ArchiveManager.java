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
package org.streamspinner;

import org.streamspinner.engine.TupleSet;
import org.streamspinner.engine.Schema;
import org.streamspinner.query.SourceSet;
import org.streamspinner.query.PredicateSet;
import org.streamspinner.query.AttributeList;

public interface ArchiveManager {

	public void init() throws StreamSpinnerException;
	public void start() throws StreamSpinnerException;
	public void stop() throws StreamSpinnerException;
	public void createTable(String table_name, Schema schema) throws StreamSpinnerException;
	public void dropTable(String table_name) throws StreamSpinnerException;
	public void insert(String table_name, TupleSet tuples) throws StreamSpinnerException;
	public void select(SourceSet sources, PredicateSet conditions, AttributeList attributes) throws StreamSpinnerException;

}
