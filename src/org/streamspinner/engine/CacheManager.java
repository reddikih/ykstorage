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

import org.streamspinner.Operators;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.query.ExecutionPlan;
import org.streamspinner.query.ANDNode;
import org.streamspinner.query.ORNode;
import org.streamspinner.query.OperatorGroup;
import java.util.HashMap;
import java.util.Iterator;

public class CacheManager {

	private int counter;
	private HashMap<Object, HashMap<ORNode, Cache>> plantable;

	public CacheManager(){
		counter = 0;
		plantable = new HashMap<Object, HashMap<ORNode, Cache>>();
	}

	public void assignCacheArea(ExecutionPlan plan) {
		HashMap<ORNode, Cache> cachetable = new HashMap<ORNode, Cache>();
		OperatorGroup[] og = plan.getOperators();
		for(int i=0; og != null && i < og.length; i++){
			if(og[i].getType().equals(Operators.ROOT) || og[i].size() == 0 || ( (! og[i].isCacheProducer() ) && (! og[i].isCacheConsumer()) ) )
				continue;
			ORNode out = og[i].getANDNodes()[0].getOutputORNode();
			if(cachetable.containsKey(out))
				continue;
			Cache c = new Cache(new Integer(counter++));
			cachetable.put(out, c);
		}
		plantable.put(plan.getPlanID(), cachetable);
	}

	public Cache getCache(ExecutionPlan plan, OperatorGroup og){
		if(og.getType().equals(Operators.ROOT) || og.size() == 0 || ( (! og.isCacheProducer() ) && (! og.isCacheConsumer() ) ) )
			return null;
		ORNode out = og.getANDNodes()[0].getOutputORNode();

		if(plantable.containsKey(plan.getPlanID())){
			HashMap<ORNode,Cache> cachetable = plantable.get(plan.getPlanID());
			if(cachetable.containsKey(out))
				return cachetable.get(out);
		}
		return null;
	}

	public void deleteCacheArea(ExecutionPlan plan){
		if(! plantable.containsKey(plan.getPlanID()))
			return;
		HashMap<ORNode,Cache> cachetable = plantable.get(plan.getPlanID());
		for(ORNode key : cachetable.keySet()){
			Cache c = cachetable.get(key);
			if(c != null)
				c.clean();
			cachetable.remove(key);
		}
		plantable.remove(plan.getPlanID());
	}

}
