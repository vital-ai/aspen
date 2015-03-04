package edu.cmu.minorthird.classify.algorithms.linear;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.swing.ButtonGroup;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import edu.cmu.minorthird.classify.BinaryClassifier;
import edu.cmu.minorthird.classify.Explanation;
import edu.cmu.minorthird.classify.Feature;
import edu.cmu.minorthird.classify.Instance;
import edu.cmu.minorthird.classify.algorithms.random.Arithmetic;
import edu.cmu.minorthird.util.MathUtil;

/** A generative Model for word-counts based on the Negative-Binomial Distribution.
 *
 * @author Edoardo Airoldi
 * Date: Jul 12, 2004
 */

public class NegativeBinomialClassifier extends BinaryClassifier implements
		Serializable{

	static final long serialVersionUID=20080130L;
	
//	static private Logger log=Logger.getLogger(PoissonClassifier.class);

	private double SCALE; // gets initialized by the NB learner

	private double priorPos; // ...

	private double priorNeg; // ...

	private SortedMap<Feature,SortedMap<String,Double>> pmsFeatureGivenPos;

	private SortedMap<Feature,SortedMap<String,Double>> pmsFeatureGivenNeg;

	public NegativeBinomialClassifier(){
		this.pmsFeatureGivenNeg=new TreeMap<Feature,SortedMap<String,Double>>();
		this.pmsFeatureGivenPos=new TreeMap<Feature,SortedMap<String,Double>>();
	}

	/** Inner product of PoissonClassifier and instance weights. */
	public double score(Instance instance){
		double totCnt=0.0;
		for(Iterator<Feature> i=instance.featureIterator();i.hasNext();){
			Feature f=i.next();
			totCnt+=instance.getWeight(f);
		}
		double score=0.0;
		for(Iterator<Feature> i=instance.featureIterator();i.hasNext();){
			Feature f=i.next();
			score+=logOddsNB(f,instance.getWeight(f),totCnt/SCALE);
		}
		score+=+Math.log(priorPos/priorNeg);
		return score;
	}

	/** Justify inner product of Negative-Binomial Classifier and instance weights. */
	public String explain(Instance instance){
		double totCnt=0.0;
		for(Iterator<Feature> i=instance.featureIterator();i.hasNext();){
			Feature f=i.next();
			totCnt+=instance.getWeight(f);
		}
		// explain
		StringBuffer buf=new StringBuffer("");
		for(Iterator<Feature> i=instance.featureIterator();i.hasNext();){
			Feature f=i.next();

			// retrieve parameters
			double mNeg;
			//double dNeg;
			double mPos;
			//double dPos;
			double x;
			
			try{
				x=instance.getWeight(f);
				SortedMap<String,Double> mdn=pmsFeatureGivenNeg.get(f);
				mNeg=((Double)mdn.get("mu")).doubleValue();
				//dNeg=((Double)mdn.get("delta")).doubleValue();
				SortedMap<String,Double> mdp=pmsFeatureGivenPos.get(f);
				mPos=((Double)mdp.get("mu")).doubleValue();
				//dPos=((Double)mdp.get("delta")).doubleValue();

				if(buf.length()>0)
					buf.append(" + ");
				buf.append(f+" <"+x+"*"+(Math.log(mPos/mNeg))+"-"+(totCnt/SCALE)+"*"+
						(+mPos-mNeg)+">");
			}catch(Exception e){
				System.out.println("warning:"+e);
			}
		}
		buf.append(" + bias<"+Math.log(priorPos/priorNeg)+">");
		buf.append(" = "+score(instance));
		return buf.toString();

	}

	public Explanation getExplanation(Instance instance){
		Explanation ex=new Explanation(explain(instance));
		return ex;
	}

	//
	// Get, Set, Check, ...
	//

	/** Set the scale term for the NB classifier to value */
	public void setScale(double value){
		this.SCALE=value;
	}

	/** Set the prior for positive documents */
	public void setPriorPos(double k,double n,double prior,double pseudoCounts){
		//System.out.println( ". "+Math.log((k+prior*pseudoCounts) / (n+pseudoCounts) ) );
		this.priorPos=(k+prior*pseudoCounts)/(n+pseudoCounts);
	}

	/** Set the prior for negative documents */
	public void setPriorNeg(double k,double n,double prior,double pseudoCounts){
		//System.out.println( ". "+Math.log((k+prior*pseudoCounts) / (n+pseudoCounts) ) );
		this.priorNeg=(k+prior*pseudoCounts)/(n+pseudoCounts);
	}

	/** compute log-odds for feature f with x counts, in an instance of weight w */
	private double logOddsNB(Feature f,double x,double w){
		// retrieve parameters
		double mNeg,dNeg,mPos,dPos,logOdds;
		try{
			SortedMap<String,Double> mdn=pmsFeatureGivenNeg.get(f);
			mNeg=((Double)mdn.get("mu")).doubleValue();
			dNeg=((Double)mdn.get("delta")).doubleValue();
			SortedMap<String,Double> mdp=pmsFeatureGivenPos.get(f);
			mPos=((Double)mdp.get("mu")).doubleValue();
			dPos=((Double)mdp.get("delta")).doubleValue();

			// compute log-odds
			if(dPos==0.0||dNeg==0.0){
				logOdds=x*(Math.log(mPos/mNeg))-w*(mPos-mNeg);
			}else{
				logOdds=
						Arithmetic.logGamma(x+mPos/dPos)-Arithmetic.logGamma(mPos/dPos)-
								Arithmetic.logGamma(x+mNeg/dNeg)+
								Arithmetic.logGamma(mNeg/dNeg)+x*Math.log(dPos/dNeg)-x*
								Math.log((1.0+w*dPos)/(1.0+w*dNeg));
			}
			//if ( new Double(logOdds).isNaN() ) { logOdds = 0.0; }
		}catch(Exception e){
			logOdds=0.0;
		}
		return logOdds;
	}

	/** Store parameters for f|negative */
	public void setPmsNeg(Feature f,SortedMap<String,Double> tmap){
		pmsFeatureGivenNeg.put(f,tmap);
	}

	/** Store parameters for f|positive */
	public void setPmsPos(Feature f,SortedMap<String,Double> tmap){
		pmsFeatureGivenPos.put(f,tmap);
	}

	private double featureScore(Feature f,String p,String c){
		double value=0.0;
		try{
			if(c.equals("POS")){
				value=pmsFeatureGivenPos.get(f).get(p);
			}else if(c.equals("NEG")){
				value=pmsFeatureGivenNeg.get(f).get(p);
			}
		}catch(Exception e){
			System.out.println("error: ... in NB.toGui.featureScore("+f+","+p+","+c+
					")");
			System.exit(1);
		}
		return value;
	}

	public Iterator<Feature> featureIterator(){
		return pmsFeatureGivenPos.keySet().iterator();
	}


	public String toString(){
		String a=pmsFeatureGivenNeg.toString();
		String b=pmsFeatureGivenPos.toString();
		return("Neg: "+a+"\n"+"Pos: "+b);
	}
}
