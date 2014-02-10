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

import java.util.*;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;

public abstract class AggregateFunctions {

	public static List<Long> asList(long[] array){
		ArrayList<Long> rval = new ArrayList<Long>(array.length);
		for(long e : array)
			rval.add(e);
		return rval;
	}

	public static List<Double> asList(double[] array){
		ArrayList<Double> rval = new ArrayList<Double>(array.length);
		for(double e : array)
			rval.add(e);
		return rval;
	}

	public static long max(long[] array){
		checkArray(array);
		return Collections.max(asList(array));
	}

	public static double max(double[] array){
		checkArray(array);
		return Collections.max(asList(array));
	}

	public static long min(long[] array){
		checkArray(array);
		return Collections.min(asList(array));
	}

	public static double min(double[] array){
		checkArray(array);
		return Collections.min(asList(array));
	}

	public static long sum(long[] array){
		//System.out.println("length: " + array.length);
		checkArray(array);
		long sum = 0;
		for(int i=0; i < array.length; i++)
			sum += array[i];
		//System.out.println("sum(Long): " + sum);
		return sum;
	}

	public static double sum(double[] array){
		checkArray(array);
		double sum = 0;
		for(int i=0; i < array.length; i++)
			sum += array[i];
		return sum;
	}
	
	// XXX
	public static long inte(long[] array) {
		checkArray(array);
		long sum = 0;
		for(int i = 0; i < array.length; i++){
			sum += array[i];
		}
		return sum * 10;
	}
	public static double inte(double[] array) {
		checkArray(array);
		double sum = 0;
		for(int i = 0; i < array.length; i++){
			sum += array[i];
		}
		return sum / (1000 / Parameter.MEMORYHILOGGER_INTERVAL);
	}
	// XXX

	public static long avg(long[] array){
		checkArray(array);
		return (long)(sum(array) / (long)(array.length));
	}

	public static double avg(double[] array){
		checkArray(array);
		return sum(array) / (double)(array.length);
	}
	
	public static long count(long[] array){
		if(array == null)
			throw new NoSuchElementException();
		return (long)(array.length);
	}

	public static long count(double[] array){
		if(array == null)
			throw new NoSuchElementException();
		return (long)(array.length);
	}

	public static long count(String[] array){
		if(array == null)
			throw new NoSuchElementException();
		return (long)(array.length);
	}

	public static long count(Object[] array){
		if(array == null)
			throw new NoSuchElementException();
		return (long)(array.length);
	}

	private static void checkArray(long[] array) throws NoSuchElementException {
		if(array == null || array.length == 0)
			throw new NoSuchElementException();
	}

	private static void checkArray(double[] array) throws NoSuchElementException {
		if(array == null || array.length == 0)
			throw new NoSuchElementException();
	}

	public static long[] array(long[] array){
		return array;
	}

	public static double[] array(double[] array){
		return array;
	}

	public static String[] array(String[] array){
		return array;
	}

	public static Object[] array(Object[] array){
		return array;
	}
}
