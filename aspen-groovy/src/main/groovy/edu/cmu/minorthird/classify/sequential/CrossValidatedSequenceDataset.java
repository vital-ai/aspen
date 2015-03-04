package edu.cmu.minorthird.classify.sequential;

import org.apache.log4j.Logger;

import edu.cmu.minorthird.classify.Dataset;
import edu.cmu.minorthird.classify.Example;
import edu.cmu.minorthird.classify.Splitter;
import edu.cmu.minorthird.classify.experiments.Evaluation;
import edu.cmu.minorthird.util.ProgressCounter;

/** 
 * View result of some sort of train/test experiment
 * on sequential data.
 *
 * @author William Cohen
 */

public class CrossValidatedSequenceDataset 
{
	static private Logger log = Logger.getLogger(CrossValidatedSequenceDataset.class);

	private ClassifiedSequenceDataset[] cds;
	private ClassifiedSequenceDataset[] trainCds;
	private Evaluation v;

	public CrossValidatedSequenceDataset(
			SequenceClassifierLearner learner,SequenceDataset d,Splitter<Example[]> splitter)
	{
		this(learner,d,splitter,false);
	}

	public CrossValidatedSequenceDataset(
			SequenceClassifierLearner learner,SequenceDataset d,Splitter<Example[]> splitter,boolean saveTrainPartitions)
	{
		Dataset.Split s = d.splitSequence(splitter);
		cds = new ClassifiedSequenceDataset[s.getNumPartitions()];
		trainCds = saveTrainPartitions ? new ClassifiedSequenceDataset[s.getNumPartitions()] : null;
		v = new Evaluation(d.getSchema());
		ProgressCounter pc = new ProgressCounter("train/test","fold",s.getNumPartitions());
		for (int k=0; k<s.getNumPartitions(); k++) {
			SequenceDataset trainData = (SequenceDataset)s.getTrain(k);
			SequenceDataset testData = (SequenceDataset)s.getTest(k);
			log.info("splitting with "+splitter+", preparing to train on "+trainData.size()
					+" and test on "+testData.size());
			//showSubpops("subpops for test fold "+k+": ", testData);
			SequenceClassifier c = new DatasetSequenceClassifierTeacher(trainData).train(learner);
			cds[k] = new ClassifiedSequenceDataset(c, testData);
			if (trainCds!=null) trainCds[k] = new ClassifiedSequenceDataset(c, trainData);
			v.extend( cds[k].getClassifier(), testData, Evaluation.DEFAULT_PARTITION_ID );
			log.info("splitting with "+splitter+", stored classified dataset");
			pc.progress();
		}
		pc.finished();
	}

	public Evaluation getEvaluation()
	{
		return v;
	}

//	private void showSubpops(String msg,SequenceDataset d)
//	{
//		Set ids = new TreeSet();
//		for (Iterator<Example> i=d.iterator(); i.hasNext(); ) {
//			Example e = i.next();
//			ids.add(e.getSubpopulationId());
//		}
//		log.debug(msg+ids.toString());
//	}

}
