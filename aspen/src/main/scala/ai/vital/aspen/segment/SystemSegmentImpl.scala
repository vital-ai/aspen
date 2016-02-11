package ai.vital.aspen.segment

import java.util.ArrayList
import java.util.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame
import ai.vital.sql.model.VitalSignsToSqlBridge
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.model.VITAL_Container
import ai.vital.aspen.data.impl.LoadDataSetTaskImpl

object SystemSegmentImpl {

  def readDataFrame(df : DataFrame) : VITAL_Container = {
    
    val blockRDD = LoadDataSetTaskImpl.dataFrameToVitalBlockRDD(df)
    
    val c = new VITAL_Container(false)
    
    for( x <- blockRDD.collect() ) {
      
       val l = VitalSigns.get().decodeBlock(x._2, 0, x._2.length) 
      
       c.putGraphObjects(l)
       
    }
    
    c
    
  }
}