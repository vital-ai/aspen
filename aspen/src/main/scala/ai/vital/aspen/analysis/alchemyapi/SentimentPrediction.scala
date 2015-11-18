package ai.vital.aspen.analysis.alchemyapi

import ai.vital.predictmodel.Prediction


object SentimentPrediction {
  
	val positive = "positive";
  
  val negative = "negative";
  
  val neutral = "neutral";
  
  
}

class SentimentPrediction extends Prediction {

  var sentimentType : String = null;
  
  var mixed : Boolean = false;
  
  var score : java.lang.Double = null;
  
}