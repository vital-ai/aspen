package iitb.CRF;

import java.io.*;
/**
 *
 * CRF (conditional random fields) This class provides support for
 * training and applying a conditional random field for sequence
 * labeling problems.   
 *
 * @author Sunita Sarawagi
 *
 */ 


public class CRF implements Serializable {
    /**
     * Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 14L;
    double lambda[];
    protected int numY;
    transient Trainer trainer;
    FeatureGenerator featureGenerator;
    EdgeGenerator edgeGen;
    HistoryManager histMgr;
    public CrfParams params;
    transient Viterbi viterbi;
    
    /**
     * @param numLabels is the number of distinct class labels or y-labels
     * @param fgen is the class that is responsible for providing 
     * the features for a particular position on the sequence.
     * @param arg is a string that can be used to control various 
     * parameters of the CRF, these are space separated name-value pairs 
     * described in 
     * @see iitb.CRF.CrfParams 
     */
    public CRF(int numLabels, FeatureGenerator fgen, String arg) {
        this(numLabels, fgen, CrfParams.stringToOptions(arg));
    }
    
    public CRF(int numLabels, FeatureGenerator fgen, java.util.Properties configOptions) {
        this(numLabels,1,fgen,configOptions);
    }
    
    public CRF(int numLabels, int histsize, FeatureGenerator fgen, java.util.Properties configOptions) {
        histMgr = new HistoryManager(histsize,numLabels);
        featureGenerator = histMgr.getFeatureGen(fgen);
        numY = histMgr.numY;
        params = new CrfParams(configOptions);
        edgeGen = histMgr.getEdgeGenerator();
        viterbi = getViterbi(1);
    }
    
    /*
     * useful for resetting Viterbi options after loading a saved model.
     */
    public void reinitOptions(java.util.Properties configOptions) {
	params = new CrfParams(configOptions);
	viterbi = null;
    }
    /**
     * write the trained parameters of the CRF to the file
     */
    public void write(String fileName)  throws IOException {
        PrintWriter out=new PrintWriter(new FileOutputStream(fileName));
        out.println(lambda.length);
        for (int i = 0; i < lambda.length; i++)
            out.println(lambda[i]);
        out.close();
    }
    /**
     * read the parameters of the CRF from a file
     */
    public void read(String fileName) throws IOException {
        BufferedReader in=new BufferedReader(new FileReader(fileName));
        int numF = Integer.parseInt(in.readLine());
        lambda = new double[numF];
        int pos = 0;
        String line;
        while((line=in.readLine())!=null) {
            lambda[pos++] = Double.parseDouble(line);
        }
    }
    protected Trainer getTrainer() {
        if (params.trainerType.startsWith("Collins"))
            return new CollinsTrainer(params);
        return new Trainer(params);
    }
    protected Viterbi getViterbi(int beamsize) {
        return new Viterbi(this,beamsize);
    }
    /**
     * Trains the model given the data
     * @return the learnt parameter value as an array
     */
    public double[] train(DataIter trainData) {
        return train(trainData,null);
    }
    /**
     * Trains the model given the data
     * @return the learnt parameter value as an array
     */
    public double[] train(DataIter trainData, Evaluator evaluator) {
        lambda = new double[featureGenerator.numFeatures()];	
        trainer = getTrainer();
        trainer.train(this, histMgr.mapTrainData(trainData), lambda, evaluator);
        return lambda;
    }
    public double[] learntWeights() {
        return lambda;
    }
    public void apply(DataSequence dataSeq) {
        if (viterbi==null)
            viterbi = getViterbi(1);
        if (params.debugLvl > 1) 
            Util.printDbg("CRF: Applying on " + dataSeq);
        viterbi.bestLabelSequence(dataSeq,lambda);
        if (histMgr != null) {
            for(int i = dataSeq.length()-1; i >= 0; i--) {
                histMgr.set_y(dataSeq, i, dataSeq.y(i));
            }
        }
    }
    public double score(DataSequence dataSeq) {
	    if (viterbi==null)
	       viterbi = getViterbi(1);
		return viterbi.viterbiSearch(dataSeq,lambda,true);
	}
};
