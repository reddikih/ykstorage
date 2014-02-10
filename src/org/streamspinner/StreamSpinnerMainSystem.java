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

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.connection.RemoteStreamServer;
import java.util.ResourceBundle;

public interface StreamSpinnerMainSystem extends Recoverable {

	public static final String PROPERTY_FILENAME = "StreamSpinner";
	public static final String PROPERTY_WRAPPERDIR = "wrapperdir";
	public static final String PROPERTY_RMILOCATION = "rmilocation";
	public static final String PROPERTY_QUERYDIR= "querydir";
	public static final String PROPERTY_USEOPTIMIZER= "useoptimizer";
	public static final String PROPERTY_OPTIMIZATIONMODE= "optimizationmode";
	public static final String PROPERTY_CACHECONSUMERTHRESHOLD= "cacheconsumerthreshold";
	public static final String PROPERTY_FUNCTIONDIR= "functiondir";
	public static final String PROPERTY_PRIMARYLOCATION= "primarylocation";

	public void addInformationSource(InformationSource is) throws StreamSpinnerException ;

	public ResourceBundle getCurrentResourceBundle();

	public InformationSourceManager getInformationSourceManager();

	public ArchiveManager getArchiveManager();

	public SystemManager getSystemManager();

	public void setArchiveManager(ArchiveManager amgr);

	public void setSystemManager(SystemManager smgr);

	public void setResourceBundle(ResourceBundle rb);

	public void shutdown() throws StreamSpinnerException ;

	public void start() throws StreamSpinnerException ;

	public boolean isAcceptable(InternalState state);

	public InternalState getInternalState();

	public void setInternalState(InternalState state) throws StreamSpinnerException;

}
