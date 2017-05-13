package ai.vital.aspen.segment;


import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.hive.HiveContext;

import ai.vital.sql.schemas.apachesparksql.SparkSQLCustomImplementation;
import ai.vital.sql.service.config.VitalServiceSqlConfig;
import ai.vital.vitalservice.config.VitalServiceConfig;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.vitalsigns.model.VITAL_Node;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.properties.Property_hasAppID;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;
import ai.vital.vitalsigns.utils.StringUtils;

/**
 * Provides access to vital-sql endpoint's spark system segment via service configuration only
 */
public class SystemSegment {

	VitalServiceSqlConfig sqlConfig;
	
	Map<String, String> segmentToTableName = new HashMap<String, String>();
	
	String dbName;
	
	public SystemSegment(VitalServiceConfig vitalServiceConfig, HiveContext context) {
	
		if( ! ( vitalServiceConfig instanceof VitalServiceSqlConfig ) ) {
			
			throw new RuntimeException("Expected an instance of " + VitalServiceSqlConfig.class.getCanonicalName());
			
		}
		
		sqlConfig = (VitalServiceSqlConfig)vitalServiceConfig;
		
		if(sqlConfig.getApp() == null) throw new RuntimeException("App not set in config");
		
		String appID = (String) sqlConfig.getApp().getRaw(Property_hasAppID.class);
		
		if(StringUtils.isEmpty(appID)) throw new RuntimeException("App ID not set in config");
		
		//read database from uri
		String u = sqlConfig.getEndpointURL();
		if(u.indexOf('?') >= 0) {
			u = u.substring(0, u.indexOf('?'));
		}
		
		if(u.endsWith("/")) u = u.substring(0, u.length() - 1);
		
		int lastSlash = u.lastIndexOf('/');
		if(lastSlash >= 0) {
			u = u.substring(lastSlash + 1); 
		}
		
		if(u.isEmpty()) throw new RuntimeException("Couldn't determine database from URL: " + sqlConfig.getEndpointURL() );
		
		//database is taken from the URI part
		dbName = u;
		if(dbName.startsWith("/")) dbName = dbName.substring(1);
		
		String systemSegmentTable = segmentToTableName(SparkSQLCustomImplementation.SYSTEM_SEGMENT_URI);
		
		Dataset<Row> df = context.table(systemSegmentTable);
		
		VITAL_Container systemSegmentContainer = SystemSegmentImpl.readDataFrame(df);
		
		VitalApp app = null;
		
		for(Iterator<VitalApp> iter = systemSegmentContainer.iterator(VitalApp.class); iter.hasNext(); ) {
			
			VitalApp a = iter.next();
			
			String _id = (String) a.getRaw(Property_hasAppID.class);
			
			if(appID.equals(_id)) {
				app =a ;
				break;
			}
			
		}
		
		if(app == null) throw new RuntimeException("App not found: " + appID);
		
		List<VITAL_Node> segments = app.getCollection("segments", GraphContext.Container, systemSegmentContainer);
		
		for(VITAL_Node seg : segments) {
			String segID = (String) seg.getRaw(Property_hasSegmentID.class);
			if(segID != null) {
				String tName = segmentToTableName(seg.getURI());
				segmentToTableName.put(segID, tName);
			}
		}
		
	}
	
	final protected static char[] hexArray = "0123456789abcdef".toCharArray();
	public static String bytesToHex(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}
	
	private String segmentToTableName(String segmentURI) {
		
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		
		byte[] digest = md5.digest(segmentURI.getBytes(StandardCharsets.UTF_8));
		
		String t = "`" + dbName + "`.`" + sqlConfig.getTablesPrefix() + bytesToHex(digest) + "`";
		
		return t;
		
	}

	public boolean segmentExists(String segmentID) {
		return segmentToTableName.containsKey(segmentID);
	}
	
	public String getSegmentTableName(String segmentID) {
		
		String tname = segmentToTableName.get(segmentID);
		
		if(tname == null) throw new RuntimeException("Table for segment: " + segmentID + " not found");
		
		return tname;
		
	}
}
