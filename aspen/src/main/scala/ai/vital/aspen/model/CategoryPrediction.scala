package ai.vital.aspen.model

import ai.vital.predictmodel.Prediction
import ai.vital.vitalsigns.model.VITAL_Category

class CategoryPrediction extends Prediction {

  var categoryID : Double = -1;
  
  var categoryURI : String = null;
  
  var category : VITAL_Category = null;
  
}