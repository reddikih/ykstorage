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
package org.streamspinner.function;

public abstract class TimeSeriesFunctions {

	public static double euclideanDistance(long[] vector1, long[] vector2){
		if(vector1 == null || vector2 == null)
			throw new IllegalArgumentException();
		if(vector1.length != vector2.length)
			return Double.NaN;
		double dist = 0;
		for(int i=0; i < vector1.length; i++){
			long diff = vector1[i] - vector2[i];
			dist += diff * diff;
		}
		return Math.sqrt(dist);
	}

	public static double euclideanDistance(double[] vector1, double[] vector2){
		if(vector1 == null || vector2 == null)
			throw new IllegalArgumentException();
		if(vector1.length != vector2.length)
			return Double.NaN;
		double dist = 0;
		for(int i=0; i < vector1.length; i++){
			double diff = vector1[i] - vector2[i];
			dist += diff * diff;
		}
		return Math.sqrt(dist);
	}

}
