/* Copyright 2003, Carnegie Mellon, All Rights Reserved */

package edu.cmu.minorthird.classify.sequential;

import edu.cmu.minorthird.classify.*;
import edu.cmu.minorthird.classify.experiments.ClassifiedDataset;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A SequenceDataset that has been classified with a
 * SequenceClassifier.
 *
 * @author William Cohen
 */

public class ClassifiedSequenceDataset
{
//	private SequenceClassifier sequenceClassifier;
	private SequenceDataset sequenceDataset;
	private Classifier adaptedClassifier;
	
	public ClassifiedSequenceDataset(SequenceClassifier sequenceClassifier,SequenceDataset sequenceDataset)
	{
//		this.sequenceClassifier = sequenceClassifier;
		this.sequenceDataset = sequenceDataset;
		this.adaptedClassifier = new AdaptedSequenceClassifier(sequenceClassifier,sequenceDataset);
	}
	public Classifier getClassifier()
	{
		return adaptedClassifier;
	}

	/** Classifies examples from the sequenceDataset, by (a) mapping an
	 * example to it position in the containing sequence (b) classifying the
	 * containing sequence - caching it if necessary.
	 */
	private static class AdaptedSequenceClassifier implements Classifier
	{
		private SequenceClassifier sequenceClassifier;
		private class Place {
			Example[] seq;
			int index;
			public Place(Example[] seq,int index) { this.seq=seq; this.index=index; }
		}
		private Map<Object,Place> instanceToPlace = new HashMap<Object,Place>();
		private Map<Example[],ClassLabel[]> classifiedSeq = new HashMap<Example[],ClassLabel[]>();
		private Map<Example[],String> explainedSeq = new HashMap<Example[],String>();

		public AdaptedSequenceClassifier(SequenceClassifier sequenceClassifier,SequenceDataset sequenceDataset)
		{
			this.sequenceClassifier = sequenceClassifier;
			for (Iterator<Example[]> i=sequenceDataset.sequenceIterator(); i.hasNext(); ) {
				Example[] seq = i.next();
				for (int j=0; j<seq.length; j++) {
					instanceToPlace.put( seq[j].getSource(), new Place(seq,j) );
				}
			}
		}
		public ClassLabel classification(Instance instance)
		{
			Place place = instanceToPlace.get(instance.getSource());
			if (place==null) 
				throw new IllegalArgumentException("instance src"+instance.getSource()+" not in "+instanceToPlace);
			ClassLabel[] labelSeq =  classifiedSeq.get(place.seq);
			if (labelSeq==null) {
				classifiedSeq.put(place.seq, (labelSeq=sequenceClassifier.classification(place.seq)) );
			}
			return labelSeq[place.index];
		}
		public String explain(Instance instance)
		{
			Place place = instanceToPlace.get(instance.getSource());
			if (place==null) 
				throw new IllegalArgumentException("no explanation available");
			String explanation =  explainedSeq.get(place.seq);
			if (explanation==null) {
				explainedSeq.put(place.seq, (explanation=sequenceClassifier.explain(place.seq)) );
			}
			return explanation;
		}
	    public Explanation getExplanation(Instance instance) {
		Place place = instanceToPlace.get(instance.getSource());
		if (place==null) 
		    throw new IllegalArgumentException("no explanation available");
		Explanation ex = sequenceClassifier.getExplanation(place.seq);
		return ex;
	    }
	}
}
