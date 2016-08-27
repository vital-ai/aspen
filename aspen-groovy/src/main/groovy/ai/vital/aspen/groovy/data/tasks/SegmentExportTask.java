package ai.vital.aspen.groovy.data.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class SegmentExportTask extends AbstractTask {

    public String outputPath = null;
    
    public String segmentID = null;
    
    public SegmentExportTask(String segmentID, String outputPath, Map<String, Object> globalParameters) {
        super(globalParameters);
        this.segmentID = segmentID;
        this.outputPath = outputPath;
    }


    @Override
    public List<String> getRequiredParams() {
        return new ArrayList<String>();
    }

    @Override
    public List<String> getOutputParams() {
        return new ArrayList<String>();
    }

}
