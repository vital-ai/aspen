/* Copyright 2003, Carnegie Mellon, All Rights Reserved */

package edu.cmu.minorthird.classify.sequential;

import java.awt.BorderLayout;
import java.io.Serializable;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.TitledBorder;

import org.apache.log4j.Logger;

import edu.cmu.minorthird.classify.ClassLabel;
import edu.cmu.minorthird.classify.Classifier;
import edu.cmu.minorthird.classify.ExampleSchema;
import edu.cmu.minorthird.classify.Explanation;
import edu.cmu.minorthird.classify.Instance;

/** 
 * A conditional markov model classifier.
 * 
 * @author William Cohen
 */

public class CMM implements ConfidenceReportingSequenceClassifier,SequenceConstants,Serializable
{
    private static final long serialVersionUID = 20080207L;

    static Logger log = Logger.getLogger(CMM.class);
    
    static private final boolean DEBUG = false;
    private BeamSearcher searcher;
    private int historySize; 
//    private String[] possibleClassLabels;
    private Classifier classifier;
    private int beamSize = 10;

    public CMM(Classifier classifier,int historySize,ExampleSchema schema)
    {
        this.searcher = new BeamSearcher(classifier,historySize,schema);
        this.classifier = classifier;
        this.historySize = historySize;
//        this.possibleClassLabels = schema.validClassNames();
    }		

    public Classifier getClassifier() { return classifier; }
    public int getHistorySize() { return historySize; }

    public ClassLabel[] classification(Instance[] sequence)
    {
        return searcher.bestLabelSequence(sequence);
    }

    public double confidence(Instance[] sequence,ClassLabel[] predictedClasses,ClassLabel[] alternateClasses,int lo,int hi)
    {
        if (predictedClasses.length!=alternateClasses.length || predictedClasses.length!=sequence.length)
            throw new IllegalArgumentException("predictedClasses, alternateClasses, sequence should be parallel arrays");
        if (lo<0 || lo>sequence.length || hi<0 || hi>sequence.length || hi<=lo)
            throw new IllegalArgumentException("lo..hi must be define a subsequence");

        searcher.doSearch(sequence,alternateClasses);
        ClassLabel[] constrainedPrediction = searcher.viterbi(0);
        double weightOfPrediction = 
            ConfidenceUtils.sumPredictedWeights(predictedClasses,0,predictedClasses.length);
        double weightOfConstrainedPrediction = 
            ConfidenceUtils.sumPredictedWeights(constrainedPrediction,0,constrainedPrediction.length);

        if (DEBUG) {
            for (int ii=0; ii<sequence.length; ii++) {
                System.out.println("pred="+predictedClasses[ii]   +"\t"+ 
                                   "conp="+constrainedPrediction[ii] +"\t"+
                                   sequence[ii].getSource());
            }
            System.out.println(weightOfPrediction +"\t"+ weightOfConstrainedPrediction + "\t diff="+(weightOfPrediction-weightOfConstrainedPrediction));
        }

        if (weightOfConstrainedPrediction>weightOfPrediction)
            throw new IllegalStateException("constrained beam search should have returned a lower-scoring prediction?");

        return weightOfPrediction-weightOfConstrainedPrediction ;
    }

    public String explain(Instance[] sequence)
    {
        return searcher.explain(sequence);
    }

    public Explanation getExplanation(Instance[] sequence) {
	Explanation.Node top = new Explanation.Node("CMM Explanation");
	Explanation.Node searcherEx = searcher.getExplanation(sequence).getTopNode();
	if(searcherEx == null)
	    searcherEx = new Explanation.Node(searcher.explain(sequence));
	top.add(searcherEx);
	Explanation ex = new Explanation(top);
	return ex;
    }

}
