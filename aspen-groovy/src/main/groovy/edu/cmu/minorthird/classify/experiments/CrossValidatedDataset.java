package edu.cmu.minorthird.classify.experiments;

import edu.cmu.minorthird.classify.*;
import edu.cmu.minorthird.classify.algorithms.trees.DecisionTreeLearner;
import edu.cmu.minorthird.util.ProgressCounter;
import org.apache.log4j.Logger;

/** 
 * View result of some sort of train/test experiment.
 *
 * @author William Cohen
 */

public class CrossValidatedDataset
{
	static private Logger log = Logger.getLogger(CrossValidatedDataset.class);

	private ClassifiedDataset[] cds;
	private ClassifiedDataset[] trainCds;
	private Evaluation v;

	public CrossValidatedDataset(ClassifierLearner learner,Dataset d,Splitter<Example> splitter)
	{
		this(learner,d,splitter,false);
	}

	public CrossValidatedDataset(ClassifierLearner learner,Dataset d,Splitter<Example> splitter,boolean saveTrainPartitions)
	{
		Dataset.Split s = d.split(splitter);
		cds = new ClassifiedDataset[s.getNumPartitions()];
		trainCds = saveTrainPartitions ? new ClassifiedDataset[s.getNumPartitions()] : null;
		v = new Evaluation(d.getSchema());
		ProgressCounter pc = new ProgressCounter("train/test","fold",s.getNumPartitions());
		for (int k=0; k<s.getNumPartitions(); k++) {
			Dataset trainData = s.getTrain(k);
			Dataset testData = s.getTest(k);
			log.info("splitting with "+splitter+", preparing to train on "+trainData.size()
							 +" and test on "+testData.size());
			Classifier c = new DatasetClassifierTeacher(trainData).train(learner);
			DatasetIndex testIndex = new DatasetIndex(testData);
			cds[k] = new ClassifiedDataset(c, testData, testIndex);
			if (trainCds!=null) trainCds[k] = new ClassifiedDataset(c, trainData, testIndex);
			v.extend( cds[k].getClassifier(), testData, k );
			v.setProperty("classesInFold"+(k+1), 
										"train: "+classDistributionString(trainData.getSchema(),new DatasetIndex(trainData))
										+"     test: "+classDistributionString(testData.getSchema(),testIndex));
			log.info("splitting with "+splitter+", stored classified dataset");
			pc.progress();
		}
		pc.finished();
	}

	private String classDistributionString(ExampleSchema schema, DatasetIndex index)
	{
		StringBuffer buf = new StringBuffer(""); 
		java.text.DecimalFormat fmt = new java.text.DecimalFormat("#####");
		for (int i=0; i<schema.getNumberOfClasses(); i++) {
			if (buf.length()>0) buf.append("; "); 
			String label = schema.getClassName(i);
			buf.append(fmt.format(index.size(label)) + " " + label);
		}
		return buf.toString();
	}


	public Evaluation getEvaluation() { return v; }

}
