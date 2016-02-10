package ai.vital.aspen.groovy.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.data.tasks.SegmentImportTask;
import ai.vital.aspen.groovy.task.AbstractTask;

public class SegmentImportProcedure {

    public List<String> inputPaths;
    
    public Map<String, Object> paramsMap;

    public String segmentID;

    private boolean overwrite;
    
    public SegmentImportProcedure(List<String> inputPaths,
            String segmentID, boolean overwrite, Map<String, Object> paramsMap) {
        super();
        this.inputPaths = inputPaths;
        this.segmentID = segmentID;
        this.overwrite = overwrite;
        this.paramsMap = paramsMap;
    }

    public List<AbstractTask> generateTasks() {
        
        List<AbstractTask> l = new ArrayList<AbstractTask>();
        
        for(String inputPath : inputPaths) {
            
            if(inputPath.startsWith("name:")) {
                throw new RuntimeException("import from namedRDD not supported yet");
            }
            
            if(inputPath.startsWith("spark-segment:")) {
                throw new RuntimeException("import from spark-segment not supported yet");
            }
            
            CheckPathTask cpt = new CheckPathTask(inputPath, paramsMap);
            
            cpt.acceptDirectories = true;
            cpt.acceptFiles = true;
            cpt.mustExist = true;
            cpt.mustnotExist = false;
            cpt.validFileExtensions = new String[]{".vital.csv", ".vital.csv.gz"};
            cpt.singleDirectory = true;
            
            l.add(cpt);
            
        }
        

        //always check if segment exists
        CheckPathTask cpt = new CheckPathTask("spark-segment:" + segmentID, paramsMap);
        cpt.mustExist = true;
        cpt.mustnotExist = false;
        cpt.acceptFiles = true;
        cpt.acceptDirectories = true;
        cpt.validFileExtensions = new String[]{};
        cpt.singleDirectory = true;
        
        l.add(cpt);
            
        
        l.add(new SegmentImportTask(inputPaths, segmentID, overwrite, paramsMap));
        
        return l;

    }
    
}
