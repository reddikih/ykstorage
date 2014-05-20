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

import java.io.FilenameFilter;
import java.io.File;
import javax.swing.filechooser.FileFilter;

public class CQFileFilter extends javax.swing.filechooser.FileFilter implements FilenameFilter {

	public boolean accept(File dir, String name){
		if(name.endsWith(".cq") || name.endsWith(".CQ"))
			return true;
		else
			return false;
	}

	public boolean accept(File f){
		String name = f.getName();
		if(name.endsWith(".cq") || name.endsWith(".CQ"))
			return true;
		return false;
	}

	public String getDescription(){
		return "Continuous Query (*.cq , *.CQ)";
	}

}
