package iitb.MaxentClassifier;
import iitb.Utils.*;
/**
 *
 * @author Sunita Sarawagi
 *
 */ 

class DataDesc {
    int numColumns;
    int numLabels = 2;
    String colSep = ",";
    DataDesc(Options opts) throws ConfigException {
	if (opts.getProperty("numLabels") != null) {
	    numLabels = opts.getInt("numLabels");
	}
	if (opts.getMandatoryProperty("numColumns") != null) {
	    numColumns = opts.getInt("numColumns");
	}
	if (opts.getProperty("separator") != null) {
	    colSep = opts.getString("separator");
	}
    }
};
