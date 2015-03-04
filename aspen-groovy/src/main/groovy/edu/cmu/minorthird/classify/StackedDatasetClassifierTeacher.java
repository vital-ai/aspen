/* Copyright 2003, Carnegie Mellon, All Rights Reserved */

package edu.cmu.minorthird.classify;

import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import edu.cmu.minorthird.classify.experiments.Tester;
import edu.cmu.minorthird.classify.relational.RealRelationalDataset;
import edu.cmu.minorthird.util.IOUtil;

/**
 * Trains a StackedClassifierLearner using the information in  a labeled relational Dataset.
 *
 * @author Zhenzhen Kou
 *
 */
public class StackedDatasetClassifierTeacher extends StackedClassifierTeacher{

	private Dataset dataset;

	private boolean activeLearning=false;

	public StackedDatasetClassifierTeacher(Dataset dataset){
		this(dataset,false);
	}

	/**
	 * @param activeLearning if true, all learning is active - ie nothing is
	 * pushed at the learner, everything must be 'pulled' via queries.
	 * if false, all examples fron the dataset are 'pushed' at the learner
	 * via addExample.
	 */
	public StackedDatasetClassifierTeacher(Dataset dataset,boolean activeLearning){
		this.dataset=dataset;
		this.activeLearning=activeLearning;
	}

	public ExampleSchema schema(){
		return dataset.getSchema();
	}

	public Map<String,Map<String,Set<String>>> getLinksMap(){
		return RealRelationalDataset.getLinksMap();
	}

	public Map<String,Set<String>> getAggregators(){
		return RealRelationalDataset.getAggregators();
	}

	public Iterator<Example> examplePool(){
		if(activeLearning){
			return Collections.EMPTY_SET.iterator();
		}
		else{
			return dataset.iterator();
		}
	}

	public Iterator<Instance> instancePool(){
		if(activeLearning){
			return Util.toInstanceIterator(dataset.iterator());
		}else if(dataset instanceof BasicDataset){
			return ((BasicDataset)dataset).iteratorOverUnlabeled();
		}else{
			return Collections.EMPTY_SET.iterator();
		}
	}

	public Example labelInstance(Instance query){
		// the label was hidden by just hiding the type
		if(query instanceof Example){
			return (Example)query;
		}
		else{
			return null;
		}
	}

	public boolean hasAnswers(){
		return activeLearning;
	}

}
