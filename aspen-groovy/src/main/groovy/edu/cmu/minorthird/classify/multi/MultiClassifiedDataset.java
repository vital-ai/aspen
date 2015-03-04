/* Copyright 2003, Carnegie Mellon, All Rights Reserved */

package edu.cmu.minorthird.classify.multi;

import java.awt.Component;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;

import edu.cmu.minorthird.classify.Feature;
import edu.cmu.minorthird.util.ProgressCounter;

/** Pairs a dataset and a classifier, for easy inspection
 * of the actions of a classifier.
 *
 * @author Cameron Williams
 */

public class MultiClassifiedDataset {

	private MultiClassifier classifier;

	private MultiDataset dataset;

	private MultiDatasetIndex index;
	
	public MultiClassifiedDataset(MultiClassifier classifier,
			MultiDataset dataset,MultiDatasetIndex index){
		this.classifier=classifier;
		this.dataset=dataset;
		this.index=index;
	}

	public MultiClassifiedDataset(MultiClassifier classifier,MultiDataset dataset){
		this(classifier,dataset,new MultiDatasetIndex(dataset));
	}

	public MultiClassifier getClassifier(){
		return classifier;
	}

	public MultiDataset getDataset(){
		return dataset;
	}

}
