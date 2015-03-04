package edu.cmu.minorthird.classify.algorithms.svm;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Iterator;

import libsvm.svm_model;
import libsvm.svm_node;
import edu.cmu.minorthird.classify.ClassLabel;
import edu.cmu.minorthird.classify.Example;
import edu.cmu.minorthird.classify.ExampleSchema;
import edu.cmu.minorthird.classify.Feature;
import edu.cmu.minorthird.classify.FeatureFactory;
import edu.cmu.minorthird.classify.Instance;
import edu.cmu.minorthird.classify.algorithms.linear.Hyperplane;

/**
 * VisibleSVM processes the svm_model from LIBSVM into recognizabled 
 * formats, and visible GUI in MINORTHIRD.
 * 
 * For example, each svm_node of svm_model is converted to
 * Feature object in MINORTHIRD corresponding to the feature information
 * stored in FeatureIdFactory. Each support vector of svm_model is 
 * converted to Example object in MINORTHIRD.
 * 
 * @author chiachi
 *
 */
public class VisibleSVM implements Serializable{
	
	static final long serialVersionUID=20071130L;

	private String[] m_classStrLabels;//Label class name

	private Example[] m_examples;//SVs info.

	private String[][] m_exampleWeightLabels;//example has different weight according to different hyperplane for multiclass svm, mxn,m=#SVs,n=#Hyperplane

	private libsvm.m3gateway m_gate;//gate to the information stored in svm_model 

	private Hyperplane[] m_hyperplanes;//hyperplane for each pair of C(n,2), n=# of class label

	private int m_posIndicator; //1, no reverse need; -1, reverse the sign of coef value

	private String[] m_hpLabels;//If it's not POS vs. NEG, store labelClassName1 vs. labelClassName2 as Hyperplane Tab Label

	/**
	 * Constructor for the svm_model returned from SVMClassifier LIBSVM training.
	 * 
	 * @param model svm_model returned from LIBSVM training
	 * @param factory FeatureFactory which has Feature's id and corresponding information before converted into svm_node in svm_problem for LIBSVM training
	 * 
	 */
	public VisibleSVM(svm_model model,FeatureFactory factory){
		this(model,factory,null);
	}

	/**
	 * Constructor for the svm_model returned from MultiClassSVMClassifier 
	 * LIBSVM training.
	 * 
	 * @param model svm_model returned from LIBSVM training
	 * @param factory FeatureFactory which has Feature's id and corresponding information before converted into svm_node in svm_problem for LIBSVM training
	 * @param schema ExampleSchema which has class label information
	 *
	 */
	public VisibleSVM(svm_model model,FeatureFactory factory,ExampleSchema schema){

		initialize(model,schema);

		setExamples(model,factory);

		if(m_hyperplanes.length>1){

			setHyperplanes();

		}else{

			setHyperplane();

		}
	}

	/**
	 * Get Support Vectors of svm_model in array of MINORTHIRD Example format.
	 * 
	 * @return Example[] array of MINORTHIRD Example consctructed from SVs of svm_model
	 * 
	 */
	public Example[] getExamples(){

		return m_examples;

	}

	/**
	 * Get Weight Labels of Example 
	 * 
	 * return m X n String array, m = # of examples, n = # of corresponding hyperplanes 
	 * 
	 */
	public String[][] getExampleWeightLabels(){

		return m_exampleWeightLabels;

	}

	/**
	 * Get Hyperplane from the Hyperplane array
	 * 
	 */
	public Hyperplane getHyperplane(int index){

		if(index<0||index>=m_hyperplanes.length){

			throw new IllegalArgumentException("out of range: "+index);

		}

		return m_hyperplanes[index];
	}

	/**
	 * Get Hyperplane label from the Hyperplane labels array
	 * 
	 */
	public String getHyperplaneLabel(int index){

		if(index<0||index>=m_hpLabels.length){

			throw new IllegalArgumentException("out of range: "+index);

		}

		return m_hpLabels[index];
	}

	/**
	 * Add Feature and it's weight to Hyperplane. In case of the input Feature 
	 * already existed in Hyperplane, reset weight of this existed Feature to be the 
	 * existing weight plus the input Feature weight.
	 * 
	 * @param exampleIndex    index in example array
	 * @param hyperplaneIndex index in hyperplane array
	 * 
	 */
	private void addInfoToHyperplane(int exampleIndex,int hyperplaneIndex){

		for(Iterator<Feature> flidx=m_examples[exampleIndex].featureIterator();flidx
				.hasNext();){

			Feature ftemp=flidx.next();

			double featureWeightTemp=m_examples[exampleIndex].getWeight(ftemp);

			featureWeightTemp*=m_examples[exampleIndex].getWeight();

			m_hyperplanes[hyperplaneIndex].increment(ftemp,featureWeightTemp);
		}
	}

	/**
	 * Initialize the member variables of the class according to the input
	 * svm_model and ExampleSchema.
	 * 
	 * @param model     svm_model returned from LIBSVM training
	 * @param schema    ExampleSchema which has class label information
	 * 
	 */
	private void initialize(svm_model model,ExampleSchema schema){
		m_gate=new libsvm.m3gateway(model);

		int[] labelTypes=m_gate.getLabelForEachClass();//numeric value of ClassLabel

		if(schema!=null){

			setClassStrLabels(schema);

		}else{

			setClassStrLabels();

		}

		m_posIndicator=
				((labelTypes.length==2&&m_classStrLabels[0]
						.equals(ExampleSchema.POS_CLASS_NAME))||labelTypes.length>2)?1:-1;

		// Construct Hyperplanes and the corresponding tab labels
		double[] rhos=m_gate.getConstantsInDecisionFunctions();

		m_hyperplanes=new Hyperplane[rhos.length];

		m_hpLabels=new String[m_hyperplanes.length];

		for(int index=0;index<m_hyperplanes.length;++index){

			m_hyperplanes[index]=new Hyperplane();

			m_hyperplanes[index].setBias((-1.0)*rhos[index]*(double)m_posIndicator);

		}
	}

	/**
	 * Set displayed labels for the example weight
	 * 
	 */
	private void initExampleWeightLabels(){

		if(m_hyperplanes.length==1){

			for(int index=0;index<m_exampleWeightLabels.length;++index){

				DecimalFormat df=new DecimalFormat("0.0000");

				m_exampleWeightLabels[index][0]=
						df.format(m_examples[index].getWeight());
			}
		}else{
			// For multiClass
			for(int index=0;index<m_exampleWeightLabels.length;++index){
				for(int idx=0;idx<m_exampleWeightLabels[0].length;++idx){

					m_exampleWeightLabels[index][idx]="null";

				}
			}
		}
	}

	/**
	 * Set Example array from the SVs of svm_model
	 * 
	 * @param model     svm_model returned from LIBSVM training
	 * @param idFactory FeatureIdFactory which has Feature's id and corresponding information
	 *                  before converted into svm_node in svm_problem for LIBSVM training
	 * 
	 */
	private void setExamples(svm_model model,FeatureFactory factory){
		svm_node[][] supportVectorsTemp=m_gate.getSVMnodes();

		//int[] labelTypes=m_gate.getLabelForEachClass();//numeric value of ClassLabel

		int[] numSVs=m_gate.getNumSVsForEachClass();//number of SVs for each different class label

		double[][] coef=m_gate.getCoefficientsForSVsInDecisionFunctions();//(numLabels-1) X (numSVs) coefficient array

		int labelIndex=0;//current label

		int upperBoundIndex=numSVs[0];//upper bound of current label index in SVs array

		int numOfSupportVectors=supportVectorsTemp.length;

		m_examples=new Example[numOfSupportVectors];

		m_exampleWeightLabels=new String[m_examples.length][m_hyperplanes.length];

		for(int index=0;index<numOfSupportVectors;++index){

			// construct class label
			ClassLabel clTemp=new ClassLabel(m_classStrLabels[labelIndex]);

			// get weight and set prefix info. of example
			double weightTemp=m_posIndicator*coef[0][index];//2 class labels, 1 raws of coef. matrix. Weight will be reset if it's MultiClass.

			// construct example
			Instance iTemp=
					SVMUtils.nodeArrayToInstance(supportVectorsTemp[index],factory);
			m_examples[index]=new Example(iTemp,clTemp,weightTemp);

			// check if next SV belongs to different class label
			if(index==(upperBoundIndex-1)&&index!=(numOfSupportVectors-1)){

				++labelIndex;

				upperBoundIndex+=numSVs[labelIndex];
			}
		}

		initExampleWeightLabels();

	}

	/**
	 * Set Hyperplane for Binary SVMClassifier
	 * 
	 */
	private void setHyperplane(){

		m_hpLabels[0]="";

		//set # of SVs
		int numOfSupportVectors=0;

		int[] numSVs=m_gate.getNumSVsForEachClass();

		for(int idx=0;idx<numSVs.length;++idx){

			numOfSupportVectors+=numSVs[idx];
		}

		// loop through each SVs
		for(int index=0;index<numOfSupportVectors;++index){

			// loop through each features in current looped example
			for(Iterator<Feature> flidx=m_examples[index].featureIterator();flidx
					.hasNext();){

				Feature ftemp=flidx.next();

				double featureWeightTemp=m_examples[index].getWeight(ftemp);//get weight of feature

				featureWeightTemp*=m_examples[index].getWeight();//times weight of example

				m_hyperplanes[0].increment(ftemp,featureWeightTemp);//store feature and its weight info into hyperplane object
			}
		}
	}

	/**
	 * Set Hyperplane array for MultiClassSVMClassifier
	 * 
	 */
	private void setHyperplanes(){

		int[] labelTypes=m_gate.getLabelForEachClass();

		double[][] coef=m_gate.getCoefficientsForSVsInDecisionFunctions();

		// Init started index for different class labels
		int[] numSVs=m_gate.getNumSVsForEachClass();

		int[] startIdx=new int[labelTypes.length];

		startIdx[0]=0;

		for(int index=1;index<startIdx.length;++index){

			startIdx[index]=startIdx[index-1]+numSVs[index-1];

		}

		// Set Hyperplane features and tab information
		DecimalFormat df=new DecimalFormat("0.0000");

		int hpIndex=0;

		for(int iIdx=0;iIdx<labelTypes.length;++iIdx){
			for(int jIdx=iIdx+1;jIdx<labelTypes.length;++jIdx){

				m_hpLabels[hpIndex]=
						m_classStrLabels[iIdx]+" vs. "+m_classStrLabels[jIdx];

				int label0svsCount=numSVs[iIdx];

				int label1svsCount=numSVs[jIdx];

				int label0startedSVindex=startIdx[iIdx];

				int label1startedSVindex=startIdx[jIdx];

				for(int exampleIdx=label0startedSVindex;exampleIdx<(label0startedSVindex+label0svsCount);++exampleIdx){

					m_examples[exampleIdx].setWeight(coef[jIdx-1][exampleIdx]);

					m_exampleWeightLabels[exampleIdx][hpIndex]=
							df.format(coef[jIdx-1][exampleIdx]);

					addInfoToHyperplane(exampleIdx,hpIndex);
				}

				for(int exampleIdx=label1startedSVindex;exampleIdx<(label1startedSVindex+label1svsCount);++exampleIdx){

					m_examples[exampleIdx].setWeight(coef[iIdx][exampleIdx]);

					m_exampleWeightLabels[exampleIdx][hpIndex]=
							df.format(coef[iIdx][exampleIdx]);

					addInfoToHyperplane(exampleIdx,hpIndex);
				}
				++hpIndex;
			}
		}
	}

	/**
	 * Convert the numerical value of binary class labels from svm_model
	 * into the corresponding class label names.
	 * 
	 */
	private void setClassStrLabels(){

		int[] numericLabels=m_gate.getLabelForEachClass();

		m_classStrLabels=new String[numericLabels.length];

		for(int index=0;index<m_classStrLabels.length;++index){

			m_classStrLabels[index]=

			(numericLabels[index]==1)?ExampleSchema.POS_CLASS_NAME:

			ExampleSchema.NEG_CLASS_NAME;
		}
	}

	/**
	 * Convert the numerical value of multi class labels from svm_model
	 * into the corresponding class label names.
	 * 
	 * @param schema ExampleSchema which has the label name and it's corresponding
	 *               numerical value.
	 */
	private void setClassStrLabels(ExampleSchema schema){

		int[] numericLabels=m_gate.getLabelForEachClass();

		m_classStrLabels=new String[numericLabels.length];

		for(int index=0;index<m_classStrLabels.length;++index){

			m_classStrLabels[index]=schema.getClassName(numericLabels[index]);

		}
	}

}