package edu.cmu.minorthird.classify.transform;

import java.io.Serializable;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

import edu.cmu.minorthird.classify.ClassLabel;
import edu.cmu.minorthird.classify.Classifier;
import edu.cmu.minorthird.classify.Explanation;
import edu.cmu.minorthird.classify.Instance;

public class TransformingClassifier implements Classifier,Serializable{

	static final long serialVersionUID=20080201L;

	private Classifier classifier;

	private InstanceTransform transformer;

	public TransformingClassifier(Classifier classifier,
			InstanceTransform transformer){
		this.classifier=classifier;
		this.transformer=transformer;
	}

	public ClassLabel classification(Instance instance){
		return classifier.classification(transformer.transform(instance));
	}

	public String explain(Instance instance){
		Instance transformedInstance=transformer.transform(instance);
		return "Transformed instance: "+transformedInstance+"\n"+
				classifier.explain(transformedInstance)+"\n";
	}

	public Explanation getExplanation(Instance instance){
		Explanation.Node top=
				new Explanation.Node("TransformingClassifier Explanation");
		Explanation.Node transformedEx=
				classifier.getExplanation(transformer.transform(instance)).getTopNode();
		top.add(transformedEx);

		Explanation ex=new Explanation(top);
		return ex;
	}

}
