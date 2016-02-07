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
import org.streamspinner.query.ORNode;

public interface InformationSource {

	public void addArrivalTupleListener(ArrivalTupleListener atl);

	public void addMetaDataUpdateListener(MetaDataUpdateListener mdul);

	public String[] getAllTableNames();

	public String getName();

	public String getParameter(String key);

	public Schema getSchema(String tablename);

	public TupleSet getTupleSet(ORNode node) throws StreamSpinnerException;

	public void init() throws StreamSpinnerException;

	public void removeArrivalTupleListener(ArrivalTupleListener atl);

	public void removeMetaDataUpdateListener(MetaDataUpdateListener mdul);

	public void setParameter(String key, String value);

	public void start() throws StreamSpinnerException ;

	public void stop() throws StreamSpinnerException;

	public String toString();

}
