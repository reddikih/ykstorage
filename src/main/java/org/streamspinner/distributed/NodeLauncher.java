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

import java.rmi.registry.*;
import javax.swing.UIManager;
import org.streamspinner.*;
import org.streamspinner.system.*;
import org.streamspinner.gui.*;

public class NodeLauncher {
	public static void main(String[] args){
		try {
			Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
			StreamSpinnerMainSystem ssms = new StreamSpinnerMainSystemImpl();

			if(args.length == 1 && args[0].equalsIgnoreCase("gui")){
				try {
					UIManager.setLookAndFeel("com.sun.java.swing.plaf.windows.WindowsLookAndFeel");
				} catch(Exception e){
					;
				}
				SystemManagerSwing sm = new SystemManagerSwing(ssms);
				NodeManagerImpl nmi = new NodeManagerImpl(ssms, sm);
				ssms.start();
				sm.setVisible(true);
			}
			else {
				SystemManagerCUI sm = new SystemManagerCUI(ssms);
				NodeManagerImpl nmi = new NodeManagerImpl(ssms, sm);
				ssms.start();
				sm.readInputs();
				System.exit(0);
			}
		} catch(Exception e){
			e.printStackTrace();
			System.exit(0);
		}
	}

}
