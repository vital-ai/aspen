package ai.vital.aspen.logic.engine

import java.io._
import alice.tuprolog._
import alice.tuprolog.event._


class PrologEngine {

  var engine = new Prolog();
    
  val listener = new OutputListener() {
     @Override def onOutput(e:OutputEvent) {
      print (e.getMsg() )
    }
}
  
  engine.addOutputListener(listener);
  
  
  
  def loadProlog(prolog : File) {
    
    var fis = new FileInputStream(prolog)
    
    engine.setTheory(new Theory(fis))
    
  }
  
  def solve(goal : String) {
    
    var info = engine.solve(goal) : SolveInfo
    
    println (info.getSolution().toString())
    
    
    
    
  }
  
  
}