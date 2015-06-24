package ai.vital.aspen.model

import ai.vital.predictmodel.Prediction
import ai.vital.vitalsigns.model.VITAL_Node

class PageRankPrediction extends Prediction {

  var uri2Rank : java.util.Map[String, Double] = null
  
}