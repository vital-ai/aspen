/* Copyright 2003, Carnegie Mellon, All Rights Reserved */

package edu.cmu.minorthird.classify;

import java.util.Collections;
import java.util.Iterator;

/**
 * Trains a ClassifierLearner using the information in  a labeled Dataset.
 *
 * @author William Cohen
 *
 */
public class DatasetClassifierTeacher extends ClassifierTeacher{

	private Dataset dataset;

	private boolean activeLearning=false;

	public DatasetClassifierTeacher(Dataset dataset){
		this(dataset,false);
	}

	/**
	 * @param activeLearning if true, all learning is active - ie nothing is
	 * pushed at the learner, everything must be 'pulled' via queries.
	 * if false, all examples fron the dataset are 'pushed' at the learner
	 * via addExample.
	 */
	public DatasetClassifierTeacher(Dataset dataset,boolean activeLearning){
		this.dataset=dataset;
		this.activeLearning=activeLearning;
	}

	public ExampleSchema schema(){
		return dataset.getSchema();
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
			// (Edoardo Airoldi)  this itearator is empty whenever there are no
			// unlabeled examples available for semi-supervised learning.
			return ((BasicDataset)dataset).iteratorOverUnlabeled();
		}else{
			return Collections.EMPTY_SET.iterator();
		}
	}

	public Example labelInstance(Instance query){
		// the label was hidden by just hiding the type
		if(query instanceof Example)
			return (Example)query;
		else
			return null;
	}

	public boolean hasAnswers(){
		return activeLearning;
	}

}
