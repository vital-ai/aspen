package ai.vital.aspen.model

import ai.vital.predictmodel.Prediction
import ai.vital.vitalsigns.model.VITAL_Category

class CategoryPrediction extends Prediction {

  var categoryID : Double = -1d;
  
  var categoryURI : String = null;
  
  var category : VITAL_Category = null;
  
  //for some type of models
  var categoryLabel : String = null;
  
  var score : Double = -1d
  
}