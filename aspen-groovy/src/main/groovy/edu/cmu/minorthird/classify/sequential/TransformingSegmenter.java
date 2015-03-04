package edu.cmu.minorthird.classify.sequential;

import java.io.Serializable;

import edu.cmu.minorthird.classify.transform.InstanceTransform;

/*package*/ class TransformingSegmenter implements Segmenter,Serializable
{
  static private final long serialVersionUID = 20080207L;
  private InstanceTransform instanceTransform;
  private Segmenter segmenter;
  public TransformingSegmenter(InstanceTransform instanceTransform,Segmenter segmenter)
  {
    this.instanceTransform = instanceTransform;
    this.segmenter = segmenter;
  }
  public Segmentation segmentation(CandidateSegmentGroup group)
  {
    return segmenter.segmentation( new SegmentTransform(instanceTransform).transform(group) );
  }
  public String explain(CandidateSegmentGroup group)
  {
    return "not implemented";
  }
}

