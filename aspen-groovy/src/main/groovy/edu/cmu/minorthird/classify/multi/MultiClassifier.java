package edu.cmu.minorthird.classify.multi;

import java.awt.Component;
import java.io.Serializable;

import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import edu.cmu.minorthird.classify.ClassLabel;
import edu.cmu.minorthird.classify.Classifier;
import edu.cmu.minorthird.classify.Explanation;
import edu.cmu.minorthird.classify.Instance;

/**
 * Interface for a multi label classifier.
 *
 * @author Cameron Williams
 */

public class MultiClassifier implements Classifier,Serializable{
	
	static final long serialVersionUID=20080130L;

	public Classifier[] classifiers;

	public MultiClassifier(Classifier[] classifiers){
		this.classifiers=classifiers;
	}

	/** Returqn a predicted type for each element of the sequence. */
	public MultiClassLabel multiLabelClassification(Instance instance){
		ClassLabel[] labels=new ClassLabel[classifiers.length];
		for(int i=0;i<classifiers.length;i++){
			labels[i]=classifiers[i].classification(instance);
		}
		MultiClassLabel multiLabel=new MultiClassLabel(labels);
		return multiLabel;
	}

	public int getNumDim(){
		return classifiers.length;
	}

	/** Give you the class label for the first dimension */
	public ClassLabel classification(Instance instance){
		ClassLabel classLabel=classifiers[0].classification(instance);
		return classLabel;
	}

	public String explain(Instance instance){
		StringBuffer buf=new StringBuffer("");
		for(int i=0;i<classifiers.length;i++){
			buf.append(classifiers[i].explain(instance));
			buf.append("\n");
		}
		buf.append("classification = "+classification(instance).toString());
		return buf.toString();
	}

	public Explanation getExplanation(Instance instance){
		Explanation.Node top=new Explanation.Node("MultiClassifier Explanation");

		for(int i=0;i<classifiers.length;i++){
			Explanation.Node classEx=
					classifiers[i].getExplanation(instance).getTopNode();
			top.add(classEx);
		}
		Explanation.Node score=
				new Explanation.Node("classification = "+
						classification(instance).toString());
		top.add(score);
		Explanation ex=new Explanation(top);
		return ex;
	}

	public Classifier[] getClassifiers(){
		return classifiers;
	}

	public String toString(){
		StringBuffer buf=new StringBuffer("[MultiClassifier:\n");
		for(int i=0;i<classifiers.length;i++){
			buf.append(classifiers[i]+"\n");
		}
		buf.append("end MultiClassifier]\n");
		return buf.toString();
	}
}
