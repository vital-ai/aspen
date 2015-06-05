package ai.vital.aspen.model

import ai.vital.predictmodel.Prediction
import java.util.ArrayList

class RecommendationPrediction extends Prediction {

  var productURIs : ArrayList[String] = null
  
  var productRatings : ArrayList[java.lang.Double] = null
  
}