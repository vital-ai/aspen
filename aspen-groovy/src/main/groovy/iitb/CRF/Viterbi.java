package iitb.CRF;


import java.io.Serializable;

import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.impl.*;
/**
 *
 * Viterbi search
 *
 * @author Sunita Sarawagi
 *
 */ 



public class Viterbi implements Serializable {
    private static final long serialVersionUID = 8122L;
    protected CRF model;
    protected int beamsize;
    Viterbi(CRF model, int bs) {
	this.model = model;
	beamsize = bs;
	if (model.params.miscOptions.getProperty("beamSize") != null)
	    beamsize = Integer.parseInt(model.params.miscOptions.getProperty("beamSize"));

    }
    protected class Entry {
        public Soln solns[]; // TODO.
        boolean valid=true;
        protected Entry() {}
        protected Entry(int beamsize, int id, int pos) {
            solns = new Soln[beamsize];
            for (int i = 0; i < solns.length; i++)
                solns[i] = newSoln(id, pos);
        }
        protected Soln newSoln(int label, int pos) {
            return new Soln(label,pos);
        }
        protected void clear() {
            valid = false;
            for (int i = 0; i < solns.length; i++)
                solns[i].clear();
        }
        protected int size() {return solns.length;}
        protected Soln get(int i) {return solns[i];}
        protected void insert(int i, float score, Soln prev) {
            for (int k = size()-1; k > i; k--) {
                solns[k].copy(solns[k-1]);
            }
            solns[i].setPrevSoln(prev,score);
        }
        protected void add(Entry e, float thisScore) {
            assert(valid);
            if (e == null) {
                add(thisScore);
                return;
            }
            int insertPos = 0;
            for (int i = 0; (i < e.size()) && (insertPos < size()); i++) {
                float score = e.get(i).score + thisScore;
                insertPos = findInsert(insertPos, score, e.get(i));
            }
            //	    print();
        }
        protected int findInsert(int insertPos, float score, Soln prev) {
            for (; insertPos < size(); insertPos++) {
                if (score >= get(insertPos).score) {
                    insert(insertPos, score, prev);
                    insertPos++;
                    break;
                }
            }
            return insertPos;
        }
        protected void add(float thisScore) {
            findInsert(0, thisScore, null);
        }
        protected int numSolns() {
            for (int i = 0; i < solns.length; i++)
                if (solns[i].isClear())
                    return i;
            return size();
        }
        public void setValid() {valid=true;}
        void print() {
            String str = "";
            for (int i = 0; i < size(); i++)
                str += ("["+i + " " + solns[i].score + " i:" + solns[i].pos + " y:" + solns[i].label+"]");
            System.out.println(str);
        }
    };

    Entry winningLabel[][];
    protected Entry finalSoln;
    protected DoubleMatrix2D Mi;
    protected DoubleMatrix1D Ri;

    void allocateScratch(int numY) {
	Mi = new DenseDoubleMatrix2D(numY,numY);
	Ri = new DenseDoubleMatrix1D(numY);
	winningLabel = new Entry[numY][];
	finalSoln = new Entry(beamsize,0,0);
    }
    double fillArray(DataSequence dataSeq, double lambda[], boolean calcScore) {
	double corrScore = 0;
	int numY = model.numY;
	for (int i = 0; i < dataSeq.length(); i++) {
	    // compute Mi.
	    Trainer.computeLogMi(model.featureGenerator,lambda,dataSeq,i,Mi,Ri,false);
	    for (int yi = 0; yi < numY; yi++) {
		winningLabel[yi][i].clear();
		winningLabel[yi][i].valid = true;
	    }
	    for (int yi = model.edgeGen.firstY(i); yi < numY; yi = model.edgeGen.nextY(yi,i)) {
		if (i > 0) {
		    for (int yp = model.edgeGen.first(yi); yp < numY; yp = model.edgeGen.next(yi,yp)) {
			double val = Mi.get(yp,yi)+Ri.get(yi);
			winningLabel[yi][i].add(winningLabel[yp][i-1], (float)val);
		    }
		} else {
		    winningLabel[yi][i].add((float)Ri.get(yi));
		}
	    }
	    if (calcScore)
		corrScore += (Ri.get(dataSeq.y(i)) + ((i > 0)?Mi.get(dataSeq.y(i-1),dataSeq.y(i)):0));
	}
	return corrScore;
    }
    
    protected void setSegment(DataSequence dataSeq, int prevPos, int pos, int label) {
        dataSeq.set_y(pos, label);
    }
    public void bestLabelSequence(DataSequence dataSeq, double lambda[]) {
        double corrScore = viterbiSearch(dataSeq, lambda,false);
        assignLabels(dataSeq);
    }
    void assignLabels(DataSequence dataSeq) {
        Soln ybest = finalSoln.get(0);
        ybest = ybest.prevSoln;
        int pos=-1;
        while (ybest != null) {
            pos = ybest.pos;
            setSegment(dataSeq,ybest.prevPos(),ybest.pos, ybest.label);
            ybest = ybest.prevSoln;
        }
        assert(pos>=0);
    }
    public double viterbiSearch(DataSequence dataSeq, double lambda[], boolean calcCorrectScore) {
	if (Mi == null) {
	    allocateScratch(model.numY);
	}
	if ((winningLabel[0] == null) || (winningLabel[0].length < dataSeq.length())) {
	    for (int yi = 0; yi < winningLabel.length; yi++) {
		winningLabel[yi] = new Entry[dataSeq.length()];
		for (int l = 0; l < dataSeq.length(); l++)
		    winningLabel[yi][l] = new Entry((l==0)?1:beamsize, yi, l);
	    }
	}
	
	double corrScore = fillArray(dataSeq, lambda,calcCorrectScore);

	finalSoln.clear();
	finalSoln.valid = true;
	for (int yi = 0; yi < model.numY; yi++) {
	    finalSoln.add(winningLabel[yi][dataSeq.length()-1], 0);
	}
	return corrScore;
    }
    int numSolutions() {return finalSoln.numSolns();}
    Soln getBestSoln(int k) {
	return finalSoln.get(k).prevSoln;
    }
};
