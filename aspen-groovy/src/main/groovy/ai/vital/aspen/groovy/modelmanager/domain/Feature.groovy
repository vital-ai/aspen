package ai.vital.aspen.groovy.modelmanager.domain

import java.io.Serializable

class Feature implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String URI
	
	private String name
		
	private String type
	
	private String value

	public String getURI() {
		return URI;
	}

	public void setURI(String uRI) {
		URI = uRI;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
