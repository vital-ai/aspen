package ai.vital.aspen.model

import java.io.InputStream
import org.apache.commons.io.IOUtils
import java.io.InputStream
import org.apache.commons.io.IOUtils
import org.apache.spark.mllib.linalg.Vector
import ai.vital.predictmodel.Prediction
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import java.nio.charset.StandardCharsets
import ai.vital.predictmodel.NumericalFeature

object AspenPageRankPredictionModel {
  
  val spark_page_rank_prediction = "spark-page-rank-prediction";
  
}

@SerialVersionUID(1L)
class AspenPageRankPredictionModel extends PredictionModel {

  
  var outputPath : String = null
  
  
  def supportedType(): String = {
    AspenPageRankPredictionModel.spark_page_rank_prediction
  }

  def deserializeModel(stream: InputStream): Unit = {
    
    val msg = IOUtils.toString(stream)
    
//      val deserializedModel : MatrixFactorizationModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
//    
//      model = deserializedModel match {
//        case x: MatrixFactorizationModel => x
//        case _ => throw new ClassCastException
//      }
      
  }

  /**
   * In collaborative filtering this method is not used
   */
  def doPredict(v: Vector): Double = {
    throw new RuntimeException("PageRank prediction does not use this method!")
  }
  
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : java.util.Map[String, Object]) : Prediction = {
    
    throw new RuntimeException("PageRank prediction does not use this method!")
//    val objects : java.util.List[GraphObject] = null
//    
//    val categoryID = doPredict(vectorizeNoLabels(vitalBlock, featuresMap))
//    
//    val category = trainedCategories.getCategories.get(categoryID.intValue())
//    
//    val pred = new CategoryPrediction
//    pred.category = category
//    pred.categoryID = categoryID
//    
//    return pred
    
  } 
  
  @Override
  def persistFiles(tempDir : File) : Unit = {

   val os = new FileOutputStream(new File(tempDir, model_bin))
   IOUtils.write("MODEL", os)
    os.close()
    
    if(error != null) {
      FileUtils.writeStringToFile(new File(tempDir, error_txt), error, StandardCharsets.UTF_8.name())
    }
    
  }
  
  @Override
  override def close() : Unit = {
    super.close()
  }
  
  @Override
  def isTestedWithTrainData() : Boolean = {
    return true;
  }
  
  def onAlgorithmConfigParam(key: String, value: java.io.Serializable): Boolean = {

    if("outputPath".equals(key)) {
      
      outputPath = value.asInstanceOf[String]
      
    } else {
      return false
    }

    return true
    
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[NumericalFeature]
  }
  
  override def validateConfig() : Unit = {
    
    if(outputPath == null || outputPath.isEmpty()) ex("outputPath not set!")    
  }
  
}