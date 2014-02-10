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

import org.streamspinner.query.ORNode;
import org.streamspinner.engine.Schema;

public interface InformationSourceManager extends Recoverable {

	public InformationSource getInformationSource(String name) throws StreamSpinnerException;

	public InformationSource[] getAllInformationSources();

	public void initAllInformationSources() throws StreamSpinnerException ;

	public void startAllInformationSources() throws StreamSpinnerException ;

	public void stopAllInformationSources() throws StreamSpinnerException ;

	public InformationSource getSourceFromTableName(String tablename) throws StreamSpinnerException ;

	public String[] getAllTableNames() throws StreamSpinnerException ;

	public Schema[] getAllSchemas() throws StreamSpinnerException ;

	public Schema getSchema(ORNode node) throws StreamSpinnerException ;

	public void addInformationSource(InformationSource is) throws StreamSpinnerException ;

	public void addInformationSource(String confstr) throws StreamSpinnerException ;

	public void removeInformationSource(InformationSource is) throws StreamSpinnerException ;

	public void removeInformationSource(String name) throws StreamSpinnerException ;

	public void setMediator(Mediator m);

	public void setSystemManager(SystemManager sm);

	public boolean isAcceptable(InternalState state);

	public InternalState getInternalState();

	public void setInternalState(InternalState state) throws StreamSpinnerException ;

}

