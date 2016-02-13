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
package org.streamspinner.system;

import org.streamspinner.*;
import org.streamspinner.connection.RemoteStreamServer;
import org.streamspinner.distributed.DurableRemoteStreamServerWrapper;
import java.rmi.RemoteException;
import java.util.regex.*;

public class BackupServerConfigurator {

	public static void copyWrapperConfigToBackupServer(StreamSpinnerMainSystem primary, RemoteStreamServer backup) throws RemoteException {
		InformationSourceManager ism = primary.getInformationSourceManager();
		for(InformationSource is : ism.getAllInformationSources())
			backup.addInformationSource(convertWrapperConfig(is));
	}

	private static String convertWrapperConfig(InformationSource is){
		String conf = is.toString();
		if(is instanceof DurableRemoteStreamServerWrapper){
			Pattern p = Pattern.compile("<parameter\\s+name=\"myurl\"\\s+value=\"(rmi:)?//([^\"/]+)/([^\"]+)\"\\s*/>");
			Matcher m = p.matcher(conf);
			if(m.find())
				conf = m.replaceFirst("<parameter name=\"myurl\" value=\"rmi://localhost/"+m.group(3)+"\" />");
		}
		return conf;
	}

}
