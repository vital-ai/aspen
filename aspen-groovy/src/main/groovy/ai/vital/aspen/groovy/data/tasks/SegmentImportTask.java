package ai.vital.aspen.groovy.data.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class SegmentImportTask extends AbstractTask {

    public List<String> inputPaths = null;
    
    public String segmentID = null;

    public boolean overwrite;
    
    public SegmentImportTask(List<String> inputPaths, String segmentID, boolean overwrite, Map<String, Object> globalParameters) {
        super(globalParameters);
        this.inputPaths = inputPaths;
        this.segmentID = segmentID;
        this.overwrite = overwrite;
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
