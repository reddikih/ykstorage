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
package org.streamspinner.distributed;

import org.streamspinner.query.Query;
import org.streamspinner.wrapper.RemoteStreamServerWrapper;
import java.io.Serializable;
import java.util.regex.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ConnectionInfo implements Serializable {

	private static Pattern pat = Pattern.compile("^(rmi:)?//([^/]+)/.*$");

	private String tablename;
	private String source;
	private String dist;
	private String query;

	public ConnectionInfo(RemoteStreamServerWrapper rssw){
		String url = rssw.getParameter("url");
		Matcher m = pat.matcher(url);
		if(m.matches())
			source = m.group(2);
		else {
			System.out.println(url);
			source = "unknown";
		}
		dist = null;
		try {
			dist = InetAddress.getLocalHost().getCanonicalHostName();
		} catch(UnknownHostException uhe){
			dist = "127.0.0.1";
		}
		tablename = rssw.getParameter("tablename");
		query = rssw.getParameter("query");
	}

	public ConnectionInfo(String tablename, String source, String dist, String query){
		this.tablename = tablename;
		this.source = source;
		this.dist = dist;
		this.query = query;
	}

	public String getTableName(){
		return tablename;
	}

	public String getSource(){
		return source;
	}

	public String getDistination(){
		return dist;
	}

	public String getQuery(){
		return query;
	}

	public void setTableName(String tablename){
		this.tablename = tablename;
	}

	public void setSource(String source){
		this.source = source;
	}

	public void setDistination(String dist){
		this.dist = dist;
	}

	public void setQuery(String query){
		this.query = query;
	}

	public int hashCode(){
		return tablename.hashCode() + source.hashCode() + dist.hashCode() + query.hashCode();
	}

	public boolean equals(Object obj){
		if(! ( obj instanceof ConnectionInfo))
			return false;
		ConnectionInfo target = (ConnectionInfo)obj;
		return tablename.equals(target.tablename) && source.equals(target.source) && dist.equals(target.dist) && query.equals(target.query);
	}

	public String toString(){
		return "\"" + query + "\"@"+source + " ----> " + "\""+tablename+"\"@" + dist;
	}

}
