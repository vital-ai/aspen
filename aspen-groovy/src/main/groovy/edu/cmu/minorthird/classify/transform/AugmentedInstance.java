package edu.cmu.minorthird.classify.transform;

import edu.cmu.minorthird.classify.*;
import edu.cmu.minorthird.util.*;

import java.util.*;

/** 
 * Add some features to an instance.
 * 
 * @author William Cohen
 */

public class AugmentedInstance implements Instance{

	private Instance instance;

	private Feature[] newFeatures;

	private double[] newValues;

	public AugmentedInstance(Instance instance,String[] newFeatureNames,
			double[] newVals){
		if(instance==null)
			throw new IllegalArgumentException("can't use null instance!");
		this.instance=instance;
		this.newFeatures=new Feature[newFeatureNames.length];
		this.newValues=new double[newVals.length];
		for(int i=0;i<newFeatureNames.length;i++){
			this.newFeatures[i]=new Feature(newFeatureNames[i]);
			this.newValues[i]=newVals[i];
		}
	}

	//
	// delegate to wrapped instance
	//
	final public Object getSource(){
		return instance.getSource();
	}

	final public String getSubpopulationId(){
		return instance.getSubpopulationId();
	}

	final public Iterator<Feature> binaryFeatureIterator(){
		return instance.binaryFeatureIterator();
	}

	//
	// extend the numeric feature set
	//

	final public Iterator<Feature> numericFeatureIterator(){
		return new UnionIterator<Feature>(new MyIterator(),instance
				.numericFeatureIterator());
	}

	final public Iterator<Feature> featureIterator(){
		return new UnionIterator<Feature>(new MyIterator(),instance
				.featureIterator());
	}
	
	final public int numFeatures(){
		return newFeatures.length+instance.numFeatures();
	}

	final public double getWeight(Feature f){
		for(int i=0;i<newFeatures.length;i++){
			if(newFeatures[i].equals(f)){
				return newValues[i];
			}
		}
		return instance.getWeight(f);
	}

	public String toString(){
		return "[AugmentedInstance: "+instance+StringUtil.toString(newFeatures)+
				StringUtil.toString(newValues)+"]";
	}

	public class MyIterator implements Iterator<Feature>{

		private int i=0;

		public void remove(){
			throw new UnsupportedOperationException("can't remove");
		}

		public boolean hasNext(){
			return i<newFeatures.length;
		}

		public Feature next(){
			return newFeatures[i++];
		}
	}

	static public void main(String[] argv){
		MutableInstance inst=new MutableInstance();
		inst.addBinary(new Feature("william"));
		double[] vals=new double[argv.length];
		for(int i=0;i<vals.length;i++)
			vals[i]=i+1;
		AugmentedInstance aug=new AugmentedInstance(inst,argv,vals);
		System.out.println(aug.toString());
		for(int i=0;i<argv.length;i++){
			Feature f=new Feature(argv[i]);
			System.out.println("weight of "+f+"="+aug.getWeight(f));
		}
		for(Iterator<Feature> i=aug.featureIterator();i.hasNext();){
			Feature f=i.next();
			System.out.println("in aug: weight of "+f+"="+aug.getWeight(f));
		}
	}
}
