package edu.cmu.minorthird.classify.semisupervised;

import java.awt.Component;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.ListCellRenderer;

import edu.cmu.minorthird.classify.BasicDataset;
import edu.cmu.minorthird.classify.Dataset;
import edu.cmu.minorthird.classify.DatasetLoader;
import edu.cmu.minorthird.classify.Example;
import edu.cmu.minorthird.classify.ExampleSchema;
import edu.cmu.minorthird.classify.FeatureFactory;
import edu.cmu.minorthird.classify.Instance;
import edu.cmu.minorthird.classify.SampleDatasets;
import edu.cmu.minorthird.classify.Splitter;
import edu.cmu.minorthird.util.Saveable;

/**
 * @author Edoardo Airoldi
 * Date: Jul 19, 2004
 */

public class SemiSupervisedDataset implements Dataset,SemiSupervisedActions,
		Saveable{

	protected List<Example> examples=new ArrayList<Example>();

	protected List<Instance> unlabeledExamples=new ArrayList<Instance>();

	protected Set<String> classNameSet=new TreeSet<String>();

	protected FeatureFactory factory=new FeatureFactory();

	public ExampleSchema getSchema(){
		ExampleSchema schema=
				new ExampleSchema((String[])classNameSet
						.toArray(new String[classNameSet.size()]));
		if(schema.equals(ExampleSchema.BINARY_EXAMPLE_SCHEMA))
			return ExampleSchema.BINARY_EXAMPLE_SCHEMA;
		else
			return schema;
	}

	//
	// methods for semi-supervised data,  part of the SemiSupervisedDataset interface
	//
	public void addUnlabeled(Instance instance){
		unlabeledExamples.add(factory.compress(instance));
	}

	public Iterator<Instance> iteratorOverUnlabeled(){
		return unlabeledExamples.iterator();
	}

	//public ArrayList getUnlabeled() { return this.unlabeledExamples; }
	public int sizeUnlabeled(){
		return unlabeledExamples.size();
	}

	public boolean hasUnlabeled(){
		return (unlabeledExamples.size()>0)?true:false;
	}

	public FeatureFactory getFeatureFactory(){
		return factory;
	}

	//
	// methods for labeled data,  part of the Dataset interface
	//

	/**
	 * Add an example to the dataset. <br>
	 * <br>
	 * This method compresses the example before adding it to the dataset.  If
	 * you don't want/need the example to be compressed then call {@link #add(Example, boolean)}
	 *
	 * @param example The Example that you want to add to the dataset.
	 */
	public void add(Example example){
		add(example,true);
	}

	/**
	 * Add an Example to the dataset. <br>
	 * <br>
	 * This method lets the caller specify whether or not to compress the example
	 * before adding it to the dataset.
	 *
	 * @param example The example to add to the dataset
	 * @param compress Boolean specifying whether or not to compress the example.
	 */
	public void add(Example example,boolean compress){
		if(compress)
			examples.add(factory.compress(example));
		else
			examples.add(example);
		classNameSet.addAll(example.getLabel().possibleLabels());
	}

	public Iterator<Example> iterator(){
		return examples.iterator();
	}

	public int size(){
		return examples.size();
	}

	public void shuffle(Random r){
		Collections.shuffle(examples,r);
	}

	public void shuffle(){
		shuffle(new Random(999));
	}

	public Dataset shallowCopy(){
		Dataset copy=new BasicDataset();
		for(Iterator<Example> i=iterator();i.hasNext();){
			copy.add(i.next());
		}
		return copy;
	}

	//
	// Implement Saveable interface.
	//
	static private final String FORMAT_NAME="Minorthird Dataset";

	public String[] getFormatNames(){
		return new String[]{FORMAT_NAME};
	}

	public String getExtensionFor(String s){
		return ".data";
	}

	public void saveAs(File file,String format) throws IOException{
		if(!format.equals(FORMAT_NAME))
			throw new IllegalArgumentException("illegal format "+format);
		DatasetLoader.save(this,file);
	}

	public Object restore(File file) throws IOException{
		try{
			return DatasetLoader.loadFile(file);
		}catch(NumberFormatException ex){
			throw new IllegalStateException("error loading from "+file+": "+ex);
		}
	}

	/** A string view of the dataset */
	public String toString(){
		StringBuffer buf=new StringBuffer("");
		for(Iterator<Example> i=this.iterator();i.hasNext();){
			Example ex=i.next();
			buf.append(ex.toString());
			buf.append("\n");
		}
		return buf.toString();
	}


	//
	// splitter
	//

	public Split split(final Splitter<Example> splitter){
		splitter.split(examples.iterator());
		return new Split(){

			public int getNumPartitions(){
				return splitter.getNumPartitions();
			}

			public Dataset getTrain(int k){
				return invertIteration(splitter.getTrain(k));
			}

			public Dataset getTest(int k){
				return invertIteration(splitter.getTest(k));
			}
		};
	}

	private Dataset invertIteration(Iterator<Example> i){
		BasicDataset copy=new BasicDataset();
		while(i.hasNext())
			copy.add(i.next());
		return copy;
	}

	//
	// test routine
	//

	public int getNumPosExamples(){
		return -1;
	}

}
