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
package org.streamspinner;

import org.streamspinner.system.*;
import org.streamspinner.gui.*;
import java.rmi.registry.*;
import javax.swing.*;

public class StreamSpinnerLauncher {

	public static void main(String[] args){
		try {
			UIManager.setLookAndFeel("com.sun.java.swing.plaf.windows.WindowsLookAndFeel");
		} catch(Exception e){
			System.out.println(e.getMessage());
		}

		try {
			Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);

			Banner bnn = new Banner();
			bnn.setVisible(true);

			StreamSpinnerMainSystem system = new StreamSpinnerMainSystemImpl();
			SystemManagerSwing manager = new SystemManagerSwing(system);
			system.start();

			manager.setVisible(true);
			bnn.setVisible(false);

		} catch(Exception e){
			e.printStackTrace();
			System.exit(0);
		}
	}
}
