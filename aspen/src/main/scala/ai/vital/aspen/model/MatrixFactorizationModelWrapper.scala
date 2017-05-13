package ai.vital.aspen.model

object MatrixFactorizationModelWrapper {}

//@serializable
class MatrixFactorizationModelWrapper extends Serializable {
  
  var rank : Int = -1
  
  var productFeatures : Array[(Int, Array[Double])] = null
  
  var userFeatures : Array[(Int, Array[Double])] = null
  

}