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

import org.streamspinner.query.OperatorGroup;
import org.streamspinner.engine.TupleSet;

public interface LogManager {

	public void clean() throws StreamSpinnerException ;
	public int estimateCacheHitRate(OperatorGroup op) throws StreamSpinnerException ;
	public void init() throws StreamSpinnerException ;
	public void receiveLog(LogEntry le) throws StreamSpinnerException;
	public void start() throws StreamSpinnerException ;
	public void stop() throws StreamSpinnerException ;

}
