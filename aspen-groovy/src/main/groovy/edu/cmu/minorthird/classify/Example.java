package edu.cmu.minorthird.classify;

import java.io.Serializable;
import java.util.Iterator;

/** An instance that is associated with a ClassLabel.  Implements the
 * Instance interface by delegating to a wrapped Instance, so
 * subclasses just need to attach the right label construct.
 *
 * @author William Cohen
 */

public class Example implements Instance,Serializable
{
	static final long serialVersionUID = 20080125L;

	protected Instance instance;
	protected ClassLabel label;
	protected double weight;

	public Example(Instance instance,ClassLabel label) 
	{
		this(instance,label,1.0);
	}
	public Example(Instance instance,ClassLabel label,double weight) 
	{
		this.instance = instance;
		this.label = label;
		this.weight = weight;
	}

	/** get the label associated with the underlying object */
	public ClassLabel getLabel() { return label; }

	/** Get the underlying object */
	final public Object getSource() { return instance.getSource(); }

	/** Get the weight assigned to a feature in the instance.
	 */
	final public double getWeight(Feature f) { return instance.getWeight(f); }

	/** Return an iterator over all binary features  */
	final public Iterator<Feature> binaryFeatureIterator() { return instance.binaryFeatureIterator(); }

	/** Return an iterator over all numeric features */
	final public Iterator<Feature> numericFeatureIterator() { return instance.numericFeatureIterator(); }

	/** Return an iterator over all features */
	final public Iterator<Feature> featureIterator() { return instance.featureIterator(); }
	
	/** Return the number of all features */
	final public int numFeatures(){ return instance.numFeatures(); }
	
	/** Get the weight of this example. */
	final public double getWeight() { return weight; }

	/** Change the weight of this example. */
	final public void setWeight(double newWeight) { this.weight=newWeight; }

	/** Return the subpopulation Id of the instance. **/
	final public String getSubpopulationId() { return instance.getSubpopulationId(); }

	/** Return an unlabeled version of the example (an Instance) */
	final public Instance asInstance() { return instance; }

	public String toString() { return "[example: "+getLabel()+" "+asInstance().toString()+"]"; }

}
