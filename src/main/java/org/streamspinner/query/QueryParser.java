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

import org.streamspinner.*;
import java.io.*;
import java.util.*;
import java.util.regex.*;

/*
 * Continuous Query parser
 */

public class QueryParser {

	private static Pattern master = Pattern.compile("\\s*MASTER\\s+(\\w+(\\s*,\\s*\\w+)*)\\s+SELECT");
	private static Pattern select = Pattern.compile("\\s*SELECT\\s+(\\S+(\\s*,\\s*\\S+)*)\\s+FROM");
	private static Pattern attr = Pattern.compile("(\\*)|(\\w+\\.\\w+)|(\\w+\\(.*\\))");
	private static Pattern from = Pattern.compile("\\s*FROM\\s(.+?)(\\s+WHERE|$)");
	private static Pattern source = Pattern.compile("(\\w+)(\\s*\\[(\\d+)\\])?");
	private static Pattern where = Pattern.compile("\\s*WHERE\\s+(\\w.*)");
	private static Pattern cond = Pattern.compile("(((\\w+\\.)\\w+)|('.*')|(\\d+\\.\\d+)|(\\d+)|(\\w+\\(.*\\)))\\s*(>|<|=|>=|<=|!=)\\s*(((\\w+\\.)\\w+)|('.*')|(\\d+\\.\\d+)|(\\d+)|(\\w+\\(.*\\)))");

	public QueryParser(){
		;
	}

	public Query parse(String qstr) throws StreamSpinnerException {
		Matcher m;

		MasterSet masterset = null;
		m = master.matcher(qstr);
		if(! m.find())
			throw new StreamSpinnerException("Parse error: MASTER clause is needed: " + qstr);
		masterset = new MasterSet(m.group(1).split("\\s*,\\s*"));
		qstr = qstr.substring(m.end(1));


		AttributeList attrlist = null;
		m = select.matcher(qstr);
		if(! m.find())
			throw new StreamSpinnerException("Parse error: SELECT clause is needed: " + qstr);
		qstr = qstr.substring(m.end(1));
		String attrstr = m.group(1);

		m = attr.matcher(attrstr);
		ArrayList tmp = new ArrayList();
		while(m.find()){
			String a = m.group();
			if(Predicate.isFunction(a))
				tmp.add((new FunctionParameter(a)).toString());
			else
				tmp.add(a);
		}
		attrlist = new AttributeList((String[])(tmp.toArray(new String[0])));


		SourceSet sourceset = new SourceSet();
		m = from.matcher(qstr);
		if(! m.find())
			throw new StreamSpinnerException("Parse error: FROM clause is needed: " + qstr);
		String froms = m.group(1);
		qstr = qstr.substring(m.end(1));

		m = source.matcher(froms);
		while(m.find()){
			String sname = m.group(1);
			String swin  = m.group(3);
			long window = Long.MAX_VALUE;
			if(swin!=null && (! swin.equals(""))){
				try{
					window = Long.parseLong(swin);
				}
				catch(Exception e){
					throw new StreamSpinnerException(e);
				}
			}
			sourceset.add(sname, window);
		}

		PredicateSet predicateset = new PredicateSet();
		m = where.matcher(qstr);
		if(m.find()){
			m = cond.matcher(qstr);
			int index = -1;
			while(m.find()){
				/*
				for(int i=0; i < 10; i++){
					System.out.println("("+i+"): " + m.group(i));
				}
				*/
				String left = m.group(1);
				String op = m.group(8);
				String right = m.group(9);
				predicateset.add(new Predicate(left, op, right));
				index = m.end(0);
			}
			if(index > 0)
				qstr = qstr.substring(index);
		}

		return new Query(masterset, attrlist, sourceset, predicateset);
	}


	public Query loadQuery(File file) throws StreamSpinnerException,IOException {
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);

		StringBuffer qstr = new StringBuffer();
		String line;
		while((line = br.readLine()) != null){
			qstr.append(line);
			qstr.append(" ");
		}

		Query rval = parse(qstr.toString());
		return rval;
	}

	public Query[] loadQueries(File querydir) throws StreamSpinnerException,IOException {
		if(! querydir.isDirectory())
			throw new IOException("parameter is not a directory!");
		FilenameFilter ff = new CQFileFilter();
		File[] qfiles = querydir.listFiles(ff);

		ArrayList qlist = new ArrayList();
		for(int i=0; i < qfiles.length; i++){
			try{
				qlist.add(loadQuery(qfiles[i]));
			} catch(Exception e){
				e.printStackTrace();
				continue;
			}
		}
		Query[] queries = new Query[qlist.size()];
		queries = (Query[])(qlist.toArray(queries));
		return queries;
	}


	public Query[] loadQueries(String dirname) throws StreamSpinnerException,IOException {
		File querydir = new File(dirname);
		if(! querydir.isDirectory())
			throw new IOException("Parameter is not a directory.");
		return loadQueries(querydir);
	}

}
