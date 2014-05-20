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


public interface TupleSet {

	public Schema getSchema() throws StreamSpinnerException ;

	public void afterLast() throws StreamSpinnerException ;

	public void beforeFirst() throws StreamSpinnerException ;

	public void close() throws StreamSpinnerException ;

	public boolean first() throws StreamSpinnerException ;

	public Tuple getTuple() throws StreamSpinnerException ;

	public boolean last() throws StreamSpinnerException ;

	public boolean next() throws StreamSpinnerException ;

	public boolean previous() throws StreamSpinnerException ;

}
