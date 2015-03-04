/* Copyright 2003, Carnegie Mellon, All Rights Reserved */

package edu.cmu.minorthird.classify.experiments;

import java.awt.Component;
import java.util.Iterator;
import java.util.List;
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

import edu.cmu.minorthird.classify.Classifier;
import edu.cmu.minorthird.classify.ClassifierLearner;
import edu.cmu.minorthird.classify.Dataset;
import edu.cmu.minorthird.classify.DatasetClassifierTeacher;
import edu.cmu.minorthird.classify.DatasetIndex;
import edu.cmu.minorthird.classify.Example;
import edu.cmu.minorthird.classify.Feature;
import edu.cmu.minorthird.classify.RandomAccessDataset;
import edu.cmu.minorthird.classify.SampleDatasets;
import edu.cmu.minorthird.classify.algorithms.linear.NaiveBayes;
import edu.cmu.minorthird.util.ProgressCounter;

/**
 * Pairs a dataset and a classifier, for easy inspection of the actions of a
 * classifier.
 * 
 * @author William Cohen
 */

public class ClassifiedDataset {

	private Classifier classifier;

	private RandomAccessDataset dataset;

	private DatasetIndex index;

	public ClassifiedDataset(Classifier classifier,Dataset dataset){
		this(classifier,dataset,new DatasetIndex(dataset));
	}

	public ClassifiedDataset(Classifier classifier,Dataset dataset,
			DatasetIndex index){
		this.classifier=classifier;
		if(dataset instanceof RandomAccessDataset){
			this.dataset=(RandomAccessDataset)dataset;
		}else{
			this.dataset=new RandomAccessDataset();
			for(Iterator<Example> i=dataset.iterator();i.hasNext();){
				this.dataset.add(i.next());
			}
		}
		this.index=index;
	}

	public Classifier getClassifier(){
		return classifier;
	}

	public Dataset getDataset(){
		return dataset;
	}


}
