package ai.vital.aspen.groovy.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.data.tasks.SegmentExportTask;
import ai.vital.aspen.groovy.task.AbstractTask;

public class SegmentExportProcedure {

    
    public Map<String, Object> paramsMap;

    public String segmentID;
    
    public String outputPath;

    private boolean overwrite;
    
    public SegmentExportProcedure(String segmentID, String outputPath, boolean overwrite, Map<String, Object> paramsMap) {
        super();
        this.outputPath = outputPath;
        this.segmentID = segmentID;
        this.overwrite = overwrite;
        this.paramsMap = paramsMap;
    }

    public List<AbstractTask> generateTasks() {
        
        List<AbstractTask> l = new ArrayList<AbstractTask>();
        
        //always check if segment exists
        CheckPathTask cpt = new CheckPathTask("spark-segment:" + segmentID, paramsMap);
        cpt.mustExist = true;
        cpt.mustnotExist = false;
        cpt.acceptFiles = true;
        cpt.acceptDirectories = true;
        cpt.validFileExtensions = new String[]{};
        cpt.singleDirectory = true;
        
        l.add(cpt);
        
        
        CheckPathTask c2 = new CheckPathTask(outputPath, paramsMap);
        
        if(outputPath.startsWith("name:")) {
            throw new RuntimeException("export to namedRDD not supported yet");
        }
            
        if(outputPath.startsWith("spark-segment:")) {
            throw new RuntimeException("export to another spark-segment not supported yet");
        }
            
        c2.acceptDirectories = true;
        c2.acceptFiles = true;
        c2.mustExist = false;
        c2.mustnotExist = !overwrite;
        c2.validFileExtensions = new String[]{".vital.csv", ".vital.csv.gz"};
        c2.singleDirectory = true;
            
        l.add(c2);
        
        if(overwrite) {
            l.add(new DeletePathTask(outputPath, paramsMap));
        }
        
        l.add(new SegmentExportTask(segmentID, outputPath, paramsMap));
        
        return l;

    }
    
}
