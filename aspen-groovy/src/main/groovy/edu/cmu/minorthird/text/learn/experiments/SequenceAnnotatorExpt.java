package edu.cmu.minorthird.text.learn.experiments;

import edu.cmu.minorthird.classify.Example;
import edu.cmu.minorthird.classify.ExampleSchema;
import edu.cmu.minorthird.classify.Splitter;
import edu.cmu.minorthird.classify.experiments.Evaluation;
import edu.cmu.minorthird.classify.experiments.Expt;
import edu.cmu.minorthird.classify.experiments.RandomSplitter;
import edu.cmu.minorthird.classify.experiments.Tester;
import edu.cmu.minorthird.classify.sequential.BatchSequenceClassifierLearner;
import edu.cmu.minorthird.classify.sequential.CrossValidatedSequenceDataset;
import edu.cmu.minorthird.classify.sequential.SequenceClassifier;
import edu.cmu.minorthird.classify.sequential.SequenceClassifierLearner;
import edu.cmu.minorthird.classify.sequential.SequenceDataset;
import edu.cmu.minorthird.text.Annotator;
import edu.cmu.minorthird.text.FancyLoader;
import edu.cmu.minorthird.text.TextLabels;
import edu.cmu.minorthird.text.learn.AnnotatorTeacher;
import edu.cmu.minorthird.text.learn.SequenceAnnotatorLearner;
import edu.cmu.minorthird.text.learn.TextLabelsAnnotatorTeacher;
import edu.cmu.minorthird.ui.Recommended;

/**
 * Run an annotation-learning experiment based on pre-labeled text , using a
 * sequence learning method, and showing the result of evaluation of the
 * sequence-classification level.
 * 
 * @author William Cohen
 */

public class SequenceAnnotatorExpt{

	private TextLabels labels;

	private Splitter<Example[]> splitter;

	private SequenceClassifierLearner learner;

	private String inputLabel;

	private String tokPropFeats;

	private SequenceDataset sequenceDataset;

	public SequenceAnnotatorExpt(TextLabels labels,Splitter<Example[]> splitter,
			SequenceClassifierLearner learner,String inputLabel){
		this(labels,splitter,learner,inputLabel,null);
	}

	public SequenceAnnotatorExpt(TextLabels labels,Splitter<Example[]> splitter,
			SequenceClassifierLearner learner,String inputLabel,String tokPropFeats){
		this.labels=labels;
		this.splitter=splitter;
		this.learner=learner;
		this.inputLabel=inputLabel;
		this.tokPropFeats=tokPropFeats;
		AnnotatorTeacher teacher=new TextLabelsAnnotatorTeacher(labels,inputLabel);
		Recommended.TokenFE fe=new Recommended.TokenFE();
		if(tokPropFeats!=null)
			fe.setTokenPropertyFeatures(tokPropFeats);
		final int size=learner.getHistorySize();
		BatchSequenceClassifierLearner dummyLearner=
				new BatchSequenceClassifierLearner(){

					public void setSchema(ExampleSchema schema){
					}

					public SequenceClassifier batchTrain(SequenceDataset dataset){
						return null;
					}

					public int getHistorySize(){
						return size;
					}
				};
		SequenceAnnotatorLearner dummy=
				new SequenceAnnotatorLearner(dummyLearner,fe){

					public Annotator getAnnotator(){
						return null;
					}
				};
		teacher.train(dummy);
		sequenceDataset=dummy.getSequenceDataset();
	}
	
	public TextLabels getLabels(){
		return labels;
	}
	
	public String getInputLabel(){
		return inputLabel;
	}

	public String getTokPropFeats(){
		return tokPropFeats;
	}

	public CrossValidatedSequenceDataset crossValidatedDataset(){
		return new CrossValidatedSequenceDataset(learner,sequenceDataset,splitter);
	}

	public Evaluation evaluation(){
		Evaluation e=Tester.evaluate(learner,sequenceDataset,splitter);
		return e;
	}

	static public SequenceClassifierLearner toSeqLearner(String learnerName){
		try{
			bsh.Interpreter interp=new bsh.Interpreter();
			interp.eval("import edu.cmu.minorthird.classify.*;");
			interp.eval("import edu.cmu.minorthird.classify.experiments.*;");
			interp.eval("import edu.cmu.minorthird.classify.algorithms.linear.*;");
			interp.eval("import edu.cmu.minorthird.classify.algorithms.trees.*;");
			interp.eval("import edu.cmu.minorthird.classify.algorithms.knn.*;");
			interp.eval("import edu.cmu.minorthird.classify.algorithms.svm.*;");
			interp.eval("import edu.cmu.minorthird.classify.sequential.*;");
			interp.eval("import edu.cmu.minorthird.classify.transform.*;");
			return (SequenceClassifierLearner)interp.eval(learnerName);
		}catch(bsh.EvalError e){
			throw new IllegalArgumentException("error parsing learnerName '"+
					learnerName+"':\n"+e);
		}
	}


	private static void usage(){
		System.out
				.println("usage: -labels labelsKey -learn learner -input inputLabel -split splitter -show all|eval");
	}
}
