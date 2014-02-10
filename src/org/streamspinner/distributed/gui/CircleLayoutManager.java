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
package org.streamspinner.distributed.gui;

import java.awt.*;

public class CircleLayoutManager implements LayoutManager,LayoutManager2 {

	public CircleLayoutManager(){
		;
	}

	public void addLayoutComponent(String name, Component comp){
		;
	}

	public void addLayoutComponent(Component comp, Object constraints){
		;
	}

	public float getLayoutAlignmentX(Container target){
		return (float)0.5;
	}

	public float getLayoutAlignmentY(Container target){
		return (float)0.5;
	}

	public void invalidateLayout(Container target){
		;
	}

	public void layoutContainer(Container parent){
		Component[] comps = parent.getComponents();
		int xmax = 0, ymax = 0;
		for(int i=0; comps != null && i < comps.length; i++){
			xmax = Math.max(xmax, comps[i].getWidth());
			ymax = Math.max(ymax, comps[i].getHeight());
		}
		int width = parent.getWidth(), height = parent.getHeight();
		Dimension size = new Dimension(width - xmax, height - ymax);
		for(int i=0; comps != null && i < comps.length; i++){
			comps[i].setSize(comps[i].getPreferredSize());
			Point location = computePosition(size, comps.length, i);
			comps[i].setLocation(location);
		}
	}

	private Point computePosition(Dimension size, int n, int i){
		Point center = new Point((int)(size.getWidth()/2), (int)(size.getHeight()/2));
		double radiusx = size.getWidth()/2;
		double radiusy = size.getHeight()/2;
		double radian = ((double)i / (double) n) * 2.0 * Math.PI - 0.5 * Math.PI;
		double x = center.getX() + radiusx * Math.cos(radian);
		double y = center.getY() + radiusy * Math.sin(radian);
		Point rval = new Point((int)x, (int)y);
		return rval;
	}

	public Dimension maximumLayoutSize(Container target){
		return new Dimension(1024, 768);
	}

	public Dimension minimumLayoutSize(Container parent){
		return new Dimension(100, 100);
	}

	public Dimension preferredLayoutSize(Container parent){
		return new Dimension(800, 600);
	}

	public void removeLayoutComponent(Component comp){
		;
	}
}
