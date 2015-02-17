package iitb.CRF;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.Vector;

import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntProcedure;

/**
 *
 * @author Sunita Sarawagi
 *
 */ 

public class SegmentViterbi extends SparseViterbi {
    protected SegmentCRF segmentModel;
    static class LabelConstraints  {
        private static final long serialVersionUID = 1L;
        ConstraintDisallowedPairs disallowedPairs;
        class Intersects implements TIntProcedure {
            int label;
            int prevLabel;
            public boolean execute(int arg0) {
                return !disallowedPairs.conflictingPair(label,arg0,(arg0==prevLabel));
            }
        }
        Intersects intersectTest = new Intersects();
        /**
         * @param pairs
         */
        public LabelConstraints(ConstraintDisallowedPairs pairs) {
            disallowedPairs = pairs;
        }
        /**
         * @param set
         * @param prevLabel
         * @param i
         * @return
         */
        private boolean valid(TIntHashSet set, int label, int prevLabel) {
            if (!conflicting(label))
                return true;
             if (disallowedPairs.conflictingPair(label,prevLabel,true))
                 return false;
             intersectTest.label = label;
             intersectTest.prevLabel = prevLabel;
             return set.forEach(intersectTest);
        }
        /**
         * @param dataSeq
         * @return
         */
        public static LabelConstraints checkConstraints(CandSegDataSequence dataSeq, LabelConstraints labelCons) {
            Iterator constraints = dataSeq.constraints(-1,dataSeq.length());
            if (constraints != null) {
    			for (; constraints.hasNext();) {
    				Constraint constraint = (Constraint)constraints.next();
    				if (constraint.type() == Constraint.PAIR_DISALLOW) {
    				    if (labelCons != null) {
    				        labelCons.disallowedPairs = (ConstraintDisallowedPairs)constraint;
    				        return labelCons;
    				    } else
    				        return new LabelConstraints((ConstraintDisallowedPairs)constraint);
    				}
    			}
            }
            return null;
        }
        /**
         * @param label
         * @return
         */
        public boolean conflicting(int label) {
            return disallowedPairs.conflicting(label);
        }
    }
    LabelConstraints labelConstraints=null;
    class SolnWithLabelsOnPath extends Soln {
        void clear() {
            super.clear();
            labelsOnPath.clear();
        }
        protected void copy(Soln soln) {
            super.copy(soln);
            labelsOnPath.clear();
            labelsOnPath.addAll(((SolnWithLabelsOnPath)soln).labelsOnPath.toArray());
        }
        private static final long serialVersionUID = 1L;
        TIntHashSet labelsOnPath;
        /**
         * @param id
         * @param p
         */
        SolnWithLabelsOnPath(int id, int p) {
            super(id, p);
            labelsOnPath = new TIntHashSet();
        }
        protected void setPrevSoln(Soln prevSoln, float score) {
            super.setPrevSoln(prevSoln,score);
            if ((prevSoln != null) && (labelConstraints != null)) {
                labelsOnPath.clear();
            	labelsOnPath.addAll(((SolnWithLabelsOnPath)prevSoln).labelsOnPath.toArray());
            	assert(labelConstraints.valid(labelsOnPath,label,prevSoln.label));
            	if (labelConstraints.conflicting(prevSoln.label))
            		labelsOnPath.add(prevSoln.label);
            }
        }       
    }
    class EntryForLabelConstraints extends Entry {
        /**
         * @param beamsize
         * @param id
         * @param pos
         */
        EntryForLabelConstraints(int beamsize, int id, int pos) {
            super();
            solns = new Soln[beamsize];
            for (int i = 0; i < solns.length; i++)
                solns[i] = new SolnWithLabelsOnPath(id, pos);
        }
        protected int findInsert(int insertPos, float score, Soln prev) {
            for (; insertPos < size(); insertPos++) {
                if (score >= get(insertPos).score) {
                    if ((prev == null) || labelConstraints.valid(((SolnWithLabelsOnPath)prev).labelsOnPath,get(insertPos).label, prev.label)) {
                        insert(insertPos, score, prev);
                        insertPos++;
                    } else if (prev != null) {
                        // System.out.println("Constraint violation");
                    }
                    break;
                }
            }
            return insertPos;
        }
    }
    class ContextForLabelConstraints extends Context {
        ContextForLabelConstraints(int numY, int beamsize, int pos) {
            super(numY, beamsize, pos);
        }
        private static final long serialVersionUID = 1L;
        public void add(int y, Entry prevSoln, float thisScore) {
            if (labelConstraints==null) {
                super.add(y,prevSoln,thisScore);
            } else {
                if (getQuick(y) == null) {
                    setQuick(y, new EntryForLabelConstraints((pos==0)?1:beamsize, y, pos)); 
                }
                super.add(y,prevSoln,thisScore);
            }
        }
    }
    protected SegmentViterbi(SegmentCRF nestedModel, int bs) {
        super(nestedModel, bs);
        this.segmentModel = nestedModel;
    }
    protected void computeLogMi(DataSequence dataSeq, int i, int ell, double lambda[]) {
        SegmentTrainer.computeLogMi((CandSegDataSequence)dataSeq,i-ell,i,segmentModel.featureGenNested,lambda,Mi,Ri);
    }
    class SegmentIter extends Iter {
        int nc;
        CandidateSegments candidateSegs;
        protected void start(int i, DataSequence dataSeq) {
            candidateSegs = (CandidateSegments)dataSeq;
            nc = candidateSegs.numCandSegmentsEndingAt(i);
        }
        protected int nextEll(int i) {
            nc--;
            if (nc >= 0)
                return i -  candidateSegs.candSegmentStart(i,nc) + 1;
            return -1;
        }
    }	
    protected Iter getIter(){return new SegmentIter();}
    /**
     * @return
     */
    int prevSegEnd = -1;
    protected double getCorrectScore(DataSequence dataSeq, int i, int ell) {
    	SegmentDataSequence data = (SegmentDataSequence)dataSeq;
    	if (data.getSegmentEnd(i-ell+1) != i)
    		return 0;
    	if ((i - ell >= 0) && (prevSegEnd != i-ell))
    		return RobustMath.LOG0;
    	prevSegEnd = i;
    	if ((labelConstraints != null) && labelConstraints.conflicting(data.y(i))) {
    		for (int segStart = 0; segStart < i-ell+1; segStart = data.getSegmentEnd(segStart)+1) {
    			int segEnd = data.getSegmentEnd(segStart);
    			if (labelConstraints.disallowedPairs.conflictingPair(data.y(i),data.y(segStart),segEnd==i-ell))
    				return RobustMath.LOG0;
    		}
    	}
    	if (model.params.debugLvl > 0) {
    	    // output features that hold
    	    segmentModel.featureGenNested.startScanFeaturesAt(dataSeq,i-ell,i);
    	    while (segmentModel.featureGenNested.hasNext()) {
    	        Feature f = segmentModel.featureGenNested.next();
    	        if (((CandSegDataSequence)data).holdsInTrainingData(f,i-ell,i)) {
    	            System.out.println("Feature " + (i-ell) + " " + i + " " + segmentModel.featureGenerator.featureName(f.index()) + " " + segmentModel.lambda[f.index()] + " " + f.value());
    	        }
    	    }
    	}
    	double val = (Ri.getQuick(dataSeq.y(i)) + ((i-ell >= 0)?Mi.get(dataSeq.y(i-ell),dataSeq.y(i)):0));
    	if (Double.isInfinite(val)) {
    	    System.out.println("Infinite score");
    	}
    	return val;
    }
    protected void setSegment(DataSequence dataSeq, int prevPos, int pos, int label) {
        ((CandSegDataSequence)dataSeq).setSegment(prevPos+1,pos, label);
    }
 
    public void singleSegmentClassScores(CandSegDataSequence dataSeq, double lambda[], TIntFloatHashMap scores) {
        viterbiSearch(dataSeq, lambda,false);
        scores.clear();
        int i = dataSeq.length()-1;
        if (i >= 0) {
            double norm	 = RobustMath.LOG0;
            
            for (int y = 0; y < context[i].size(); y++) {
                if (context[i].entryNotNull(y)) {
                    Soln soln = ((Entry)context[i].getQuick(y)).get(0);
                    assert (soln.prevSoln == null); // only applicable for single segment.
                    norm = RobustMath.logSumExp(norm,soln.score);
                }
            }
            for (int y = 0; y < context[i].size(); y++) {
                if (context[i].entryNotNull(y)) {
                    Soln soln = ((Entry)context[i].getQuick(y)).get(0);
                    scores.put(soln.label,(float)Math.exp(soln.score-norm));
                }
            }
            /*context[i].getNonZeros(validPrevYs, prevContext);
           
            for (int prevPx = 0; prevPx < validPrevYs.size(); prevPx++) {
                Soln soln = ((Entry)prevContext.getQuick(prevPx)).get(0);
                assert (soln.prevSoln == null); // only applicable for single segment.
                norm = RobustMath.logSumExp(norm,soln.score);
            }
            for (int prevPx = 0; prevPx < validPrevYs.size(); prevPx++) {
                Soln soln = ((Entry)prevContext.getQuick(prevPx)).get(0);
                scores.put(soln.label,(float)Math.exp(soln.score-norm));
            }
            */
        }
    }
    protected Context newContext(int numY, int beamsize, int pos){
        if (labelConstraints == null)
            return new Context(numY,beamsize,pos);        
        return  new ContextForLabelConstraints(numY,(beamsize==1)?20:beamsize,pos); 
    }
    public double viterbiSearch(DataSequence dataSeq, double[] lambda,
            boolean calcCorrectScore) {
        labelConstraints = LabelConstraints.checkConstraints((CandSegDataSequence)dataSeq, labelConstraints);
        return super.viterbiSearch(dataSeq, lambda, calcCorrectScore);
    }
    
    class SegmentationImpl implements Segmentation {
        class Segment implements Comparable {
            int start;
            int end;
            int label;
            int id;
            Segment(int start, int end, int label) {
                this.start = start;
                this.end = end;
                this.label = label;
            }
            /* (non-Javadoc)
             * @see java.lang.Comparable#compareTo(java.lang.Object)
             */
            public int compareTo(Object arg0) {
                return end - ((Segment)arg0).end;
            }
        }
        TreeSet segments = new TreeSet();
        Segment segmentArr[]=null;
        Segment dummySegment = new Segment(0,0,0);
        /* (non-Javadoc)
         * @see iitb.CRF.Segmentation#numSegments()
         */
        public int numSegments() {
            return segments.size();
        }

        /* (non-Javadoc)
         * @see iitb.CRF.Segmentation#segmentLabel(int)
         */
        public int segmentLabel(int segmentNum) {
            return segmentArr[segmentNum].label;
        }

        /* (non-Javadoc)
         * @see iitb.CRF.Segmentation#segmentStart(int)
         */
        public int segmentStart(int segmentNum) {
            return segmentArr[segmentNum].start;
        }

        /* (non-Javadoc)
         * @see iitb.CRF.Segmentation#segmentEnd(int)
         */
        public int segmentEnd(int segmentNum) {
            return segmentArr[segmentNum].end;
        }

        /* (non-Javadoc)
         * @see iitb.CRF.Segmentation#getSegmentId(int)
         */
        public int getSegmentId(int offset) {
            dummySegment.end = offset;
//            if (segments.headSet(dummySegment) == null)
  //              return 0;
            return ((Segment)segments.tailSet(dummySegment).first()).id;
        }

        /* (non-Javadoc)
         * @see iitb.CRF.Segmentation#setSegment(int, int, int)
         */
        public void setSegment(int segmentStart, int segmentEnd, int label) {
            Segment segment = new Segment(segmentStart, segmentEnd, label);
            boolean b = segments.add(segment);
        }
        public void doneAdd() {
            segmentArr = new Segment[segments.size()];
            int p = 0;
            for (Iterator iter = segments.iterator(); iter.hasNext();) {
                segmentArr[p++] = (Segment) iter.next();
            }
            for (int i = segmentArr.length-1; i >= 0; segmentArr[i].id = i, i--);
        }
    };
    public Segmentation[] segmentSequences(CandSegDataSequence dataSeq, double lambda[], int numLabelSeqs) {
        viterbiSearch(dataSeq, lambda,false);
        int numSols = Math.min(finalSoln.numSolns(), numLabelSeqs);
        Segmentation segments[] = new Segmentation[numSols];
        for (int k = numSols-1; k >= 0; k--) {
            Soln ybest = finalSoln.get(k);
            ybest = ybest.prevSoln;
            segments[k] = new SegmentationImpl();
            while (ybest != null) {	
                segments[k].setSegment(ybest.prevPos()+1,ybest.pos,ybest.label);
                ybest = ybest.prevSoln;
            }
            ((SegmentationImpl)segments[k]).doneAdd();
        }
        return segments;
    }
};
