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
package org.streamspinner.wrapper;

import org.streamspinner.StreamSpinnerException;
import org.streamspinner.InformationSource;
import java.lang.reflect.*;
import java.util.*;
import java.io.*;
import javax.xml.parsers.*;
import org.xml.sax.*;
import org.w3c.dom.*;

public class InformationSourceParser {

	private DocumentBuilder db;

	protected class SourceFileFilter implements FilenameFilter {
		public boolean accept(File dir, String name){
			return name.endsWith(".xml") || name.endsWith(".XML");
		}
	}

	public InformationSourceParser() throws StreamSpinnerException {
		try{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			db = dbf.newDocumentBuilder();
		} catch(ParserConfigurationException pce){
			throw new StreamSpinnerException(pce);
		}
	}

	public InformationSource[] parseAll(String confdir) throws StreamSpinnerException {
		ArrayList slist = new ArrayList();

		try{
			File dir = new File(confdir);
			if(! dir.isDirectory())
				throw new IOException("Parameter is not a directory.");
			File[] sourcefiles = dir.listFiles(new SourceFileFilter());
			for(int i=0; sourcefiles != null && i < sourcefiles.length; i++)
				slist.add(parseFile(sourcefiles[i]));
		}
		catch(Exception e){
			throw new StreamSpinnerException(e);
		}

		InformationSource[] sources = (InformationSource[])slist.toArray(new InformationSource[0]);
		return sources;
	}

	public InformationSource parseFile(File sourcefile) throws StreamSpinnerException {
		try {
			Document d = db.parse(sourcefile);
			return analyzeDOM(d);
		} catch(Exception e){
			throw new StreamSpinnerException(e);
		}
	}

	public InformationSource parseString(String confstr) throws StreamSpinnerException {
		try {
			StringReader sr = new StringReader(confstr);
			InputSource is = new InputSource(sr);
			Document d = db.parse(is);
			return analyzeDOM(d);
		} catch(Exception e){
			throw new StreamSpinnerException(e);
		}
	}

	private InformationSource analyzeDOM(Document d) throws Exception {
		Element root = d.getDocumentElement();

		if(! root.getNodeName().equals("wrapper"))
			throw new StreamSpinnerException("Root node must be <wrapper>: " + root.getNodeName());

		NamedNodeMap nnmsource = root.getAttributes();

		String name = nnmsource.getNamedItem("name").getNodeValue();
		String classname = nnmsource.getNamedItem("class").getNodeValue();

		Class cls = Class.forName(classname);
		Class[] signature = { Class.forName("java.lang.String") };
		Constructor cons = cls.getConstructor(signature);
		Object[] params = { new String(name) };
		Object instance = cons.newInstance(params);

		if(! (instance instanceof org.streamspinner.InformationSource))
			throw new StreamSpinnerException("" + classname + " is not InformationSource");

		InformationSource rval = (InformationSource)instance;

		NodeList nlist = root.getChildNodes();
		for(int i=0; i < nlist.getLength(); i++){
			Node n = nlist.item(i);
			if(n.getNodeName().equals("parameter")){
				NamedNodeMap nnmparam = n.getAttributes();
				String key = nnmparam.getNamedItem("name").getNodeValue();
				String value = nnmparam.getNamedItem("value").getNodeValue();
				rval.setParameter(key, value);
			}
		}
		return rval;
	}

}
