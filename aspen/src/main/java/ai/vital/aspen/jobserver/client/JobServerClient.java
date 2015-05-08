package ai.vital.aspen.jobserver.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Simple job server client
 * @author Derek
 *
 */
public class JobServerClient {

	private HttpClient httpClient;
	private String urlBase;
	
	private static ObjectMapper jsonMapper = new ObjectMapper();
	
	/**
	 * @param _urlBase ending slash is trimmed, http:// prefix is optional
	 */
	public JobServerClient(String _urlBase) {
		
		if(_urlBase==null) throw new NullPointerException("Null urlBase"); 
		this.urlBase = _urlBase;
		while( urlBase.endsWith("/") ) {
			urlBase.substring(0, urlBase.length() - 1);
		}
		
		if(!urlBase.startsWith("http://")) {
			urlBase = "http://" + urlBase;
		}
		
		//simple default client
		httpClient = new HttpClient();
		
	}

	public String getUrlBase() {
		return urlBase;
	}
	
	//returns json structure at this moment
	public List<LinkedHashMap<String, Object>> jobs_get() throws Exception {
		
		GetMethod getMethod = new GetMethod(urlBase + "/jobs");
		
		return jsonImpl(getMethod, List.class);
		
	}
	
	
	public LinkedHashMap<String, Object> jobs_get_details(String jobID) throws Exception {
		
		GetMethod getMethod = new GetMethod(urlBase + "/jobs/" + en(jobID));
		
		return jsonImpl(getMethod, LinkedHashMap.class);
		
	}
	
	/* does not work yet
	public LinkedHashMap<String, Object> getJobConfig(String jobID) throws Exception {
		
		GetMethod getMethod = new GetMethod(urlBase + "/jobs/" + en(jobID) + "/config");
		
		return jsonImpl(getMethod, LinkedHashMap.class);
		
	}
	*/
	
	public LinkedHashMap<String, Object> jobs_post(String appName, String classPath, String context, Boolean sync, String paramsString) throws Exception {
		
		required("appName", appName);
		required("classPath", classPath);
		required("paramsString", paramsString);
		
		String url = urlBase + "/jobs";
		
		url = append(url, "appName", appName);
		url = append(url, "classPath", classPath);
		
		if(context != null) {
			url = append(url, "context", context);
		}
		
		if(sync != null) {
			url = append(url, "sync", sync.toString());
		}
		
		PostMethod postMethod = new PostMethod(url);
		
		postMethod.setRequestEntity(new StringRequestEntity(paramsString, null, StandardCharsets.UTF_8.name()));
		
		return jsonImpl(postMethod, LinkedHashMap.class);
		
	}
	
	public LinkedHashMap<String, Object> jobs_delete(String jobID) throws Exception {
		DeleteMethod deleteMethod = new DeleteMethod(urlBase + "/jobs/" + en(jobID));
		return jsonImpl(deleteMethod, LinkedHashMap.class);
	}
	
	
	
	public List<String> contexts_get() throws Exception {
		
		GetMethod getMethod = new GetMethod(urlBase + "/contexts");
		return jsonImpl(getMethod, List.class);
		
	}
	
	public String contexts_post(String name, String contextParamsURLEncoded) throws Exception {
		String url = urlBase + "/contexts/" + en(name);
		/*
		if(contextParams != null) {
			if(contextParams.length % 2 != 0) throw new Exception("expected even context params count (pairs)");
			
			for(int i =0 ; i < contextParams.length; i+= 2) {
				url = append(url, contextParams[i], contextParams[i+1]);
			}
			
		}
		*/
		
		if(contextParamsURLEncoded != null && !contextParamsURLEncoded.isEmpty()) {
			url = url + "?" + contextParamsURLEncoded;
		}
		
		PostMethod postMethod = new PostMethod(url);
		return textImpl(postMethod);
	}
	
	public String contexts_delete(String name) throws Exception {
		DeleteMethod deleteMethod = new DeleteMethod(urlBase + "/contexts/" + en(name));
		return textImpl(deleteMethod);
	}
	
	
	/**
	 * returns object with name:date string pairs
	 * @return
	 * @throws Exception
	 */
	public LinkedHashMap<String, Object> jars_get() throws Exception {
		GetMethod getMethod = new GetMethod(urlBase + "/jars");
		return jsonImpl(getMethod, LinkedHashMap.class);
	}
//	GET /jars            - lists all the jars and the last upload timestamp
//	POST /jars/<appName> - uploads a new jar under <appName>
	
	public String jars_post(String appName, InputStream jarInputStream) throws Exception {
		
		PostMethod postMethod = new PostMethod(urlBase + "/jars/" + en(appName));
		postMethod.setRequestEntity(new InputStreamRequestEntity(jarInputStream));
		return textImpl(postMethod);
		
	}
	
	/*
	public String deleteJar(String appName) throws Exception {
		
		DeleteMethod deleteMethod = new DeleteMethod(urlBase + "/jars/" + en(appName));
		
		return textImpl(deleteMethod);
		
	}
	*/
	
	
	
	private String append(String url, String n, String v) throws Exception {

		char link = '?';
		if(url.contains("?")) {
			link = '&';
		}
		
		return url + link + en(n) + "=" + en(v);
		
		
	}

	private String  en(String v) throws Exception {
		return URLEncoder.encode(v, "UTF-8");
	}
	
	private void required(String name, Object value) throws Exception {

		if(value == null) throw new Exception(name + " is required");
		if(value instanceof String && ((String)value).isEmpty()) throw new Exception(name + " is required");
		
		
	}

//	mments
//	bin script spark-jobs to get/post/delete jobs (instead of using curl) to jobserver 

	private <T> T jsonImpl(HttpMethodBase method, Class<T> returnedType) throws Exception {

		InputStream s = null;
		
		try {
		
			int status = httpClient.executeMethod(method);
			
			if(status < 200 || status > 299) {
				
				String erroMsg = null; 
				try {
					erroMsg = method.getResponseBodyAsString(8192);
				} catch(Exception e) {}
				
				throw new Exception("HTTP status: " + status + " - " + erroMsg);
				
			}
			
			s = method.getResponseBodyAsStream();
			
			return (T) jsonMapper.readValue(s, returnedType);
			
		} finally {
			IOUtils.closeQuietly(s);
			method.releaseConnection();
		}
		
	}
	
	private String textImpl(HttpMethodBase method) throws Exception {
		
		try {
		
			int status = httpClient.executeMethod(method);
			
			if(status < 200 || status > 299) {
				
				String erroMsg = null; 
				try {
					erroMsg = method.getResponseBodyAsString(8192);
				} catch(Exception e) {}
				
				throw new Exception("HTTP status: " + status + " - " + erroMsg);
				
			}
			
			return method.getResponseBodyAsString(1024*1024);
		} finally {
			method.releaseConnection();
		}
		
	}

	
	
}
