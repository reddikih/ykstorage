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
package org.streamspinner.query;

import java.util.Set;

public interface ExecutionPlan {

	public String getPlanID();

	public Set getQueryIDSet();

	public ORNode[] getORNodes();

	public boolean isTriggered(String master);

	public String[] getTriggers();

	public OperatorGroup[] getOperators();

	public OperatorGroup[] getOperatorsOnMaster(String master);

	public ORNode getBaseORNode(String source);

}
