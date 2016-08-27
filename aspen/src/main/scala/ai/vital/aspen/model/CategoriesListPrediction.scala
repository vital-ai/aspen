package ai.vital.aspen.model

import java.util.ArrayList
import java.util.List
import ai.vital.predictmodel.Prediction

class CategoriesListPrediction extends Prediction {

  var predictions : List[CategoryPrediction] = new ArrayList[CategoryPrediction]()
  
}