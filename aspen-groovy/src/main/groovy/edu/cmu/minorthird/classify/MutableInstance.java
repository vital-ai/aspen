/* Copyright 2003, Carnegie Mellon, All Rights Reserved */

package edu.cmu.minorthird.classify;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import edu.cmu.minorthird.util.UnionIterator;

/** 
 * A single instance for a learner. 
 *
 * @author William Cohen
 */

public class MutableInstance extends AbstractInstance{

	private Set<Feature> binarySet=new TreeSet<Feature>();
	private WeightedSet<Feature> numericSet=new WeightedSet<Feature>();
	
	public MutableInstance(Object source,String subpopulationId){ 
		this.source=source; 
		this.subpopulationId=subpopulationId;
	}
	
	public MutableInstance(Object source){
		this(source,null);
	}

	public MutableInstance(){
		this("_unknownSource_");
	}

	/** 
	 * Add a numeric feature. This also deletes the binary version of
	 * feature, if it exists.
	 */
	public void addNumeric(Feature feature,double value){ 
		binarySet.remove(feature);
		numericSet.add(feature,value); 
	}

	/** Add a binary feature. */
	public void addBinary(Feature feature){
		binarySet.add(feature);
	}

	/** Get the weight assigned to a feature in this instance. */
	public double getWeight(Feature feature){
		if(binarySet.contains(feature)){
			return 1.0;
		}
		else{
			return numericSet.getWeight(feature);
		}
	}
	
	/** Return an iterator over all binary features */
	public Iterator<Feature> binaryFeatureIterator(){
		return binarySet.iterator();
	}

	/** Return an iterator over all numeric features */
	public Iterator<Feature> numericFeatureIterator(){
		return numericSet.iterator();
	}

	/** Return an iterator over all features */
	public Iterator<Feature> featureIterator(){
		return new UnionIterator<Feature>(binaryFeatureIterator(),numericFeatureIterator());
	}
	
	public int numFeatures(){
		return binarySet.size()+numericSet.size();
	}

}


