package edu.cmu.minorthird.classify;

import java.util.Iterator;

/**
 * Common code for all instance implementations
 * @author ksteppe
 */
public abstract class AbstractInstance implements Instance {
	
  protected Object source;
  protected String subpopulationId;

  /** Return the underlying object being represented. */
	public Object getSource() { return source; }

  /** Return the subpopulation from which the source was drawn. */
	public String getSubpopulationId() { return subpopulationId; }

  /** Debugging view of an instance. */
	public String toString(){
		StringBuilder buf=new StringBuilder("[instance/"+subpopulationId+":");
		for(Iterator<Feature> i=binaryFeatureIterator();i.hasNext();){
			buf.append(" "+i.next());
		}
		for (Iterator<Feature> i=numericFeatureIterator();i.hasNext();){
			Feature f = i.next();
			buf.append(" "+f+":"+getWeight(f));
		}
		buf.append("]");
		return buf.toString();
	}

}
