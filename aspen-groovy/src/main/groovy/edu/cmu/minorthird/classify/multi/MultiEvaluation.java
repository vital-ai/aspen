/* Copyright 2003, Carnegie Mellon, All Rights Reserved */

package edu.cmu.minorthird.classify.multi;

import javax.swing.JComponent;

import edu.cmu.minorthird.classify.Classifier;
import edu.cmu.minorthird.classify.Dataset;
import edu.cmu.minorthird.classify.ExampleSchema;
import edu.cmu.minorthird.classify.experiments.Evaluation;
import edu.cmu.minorthird.util.ProgressCounter;

/** 
 * Stores some detailed results of evaluating a classifier on data with multiple labels.
 *
 * @author Cameron Williams
 */

public class MultiEvaluation {

	Evaluation[] evals;

	MultiExampleSchema schema;

	/** Create an evaluation for databases with this schema */

	public MultiEvaluation(MultiExampleSchema schema){
		this.schema=schema;
		ExampleSchema[] exSchemas=schema.getSchemas();
		evals=new Evaluation[exSchemas.length];
		for(int i=0;i<evals.length;i++){
			evals[i]=new Evaluation(exSchemas[i]);
		}
	}

	/** Test the classifier on the examples in the dataset and store the results. */
	public void extend(MultiClassifier c,MultiDataset d){
		ProgressCounter pc=new ProgressCounter("classifying","example",d.size());
		Classifier[] classifiers=c.getClassifiers();
		Dataset[] datasets=d.separateDatasets();
		for(int i=0;i<evals.length;i++){
			evals[i].extend(classifiers[i],datasets[i],1);
		}
		pc.progress();
		pc.finished();
	}

	/** Print summary statistics
	 */
	public void summarize(){
		for(int i=0;i<evals.length;i++){
			System.out.println("Dimension: "+i);
			double[] stats=evals[i].summaryStatistics();
			String[] statNames=evals[i].summaryStatisticNames();
			int maxLen=0;
			for(int j=0;j<statNames.length;j++){
				maxLen=Math.max(statNames[j].length(),maxLen);
			}
			for(int j=0;j<statNames.length;j++){
				System.out.print(statNames[j]+": ");
				for(int k=0;k<maxLen-statNames[j].length();k++)
					System.out.print(" ");
				System.out.println(stats[j]);
			}
		}
	}

}
