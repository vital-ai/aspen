package iitb.Model;
import java.io.Serializable;
import java.util.StringTokenizer;

public class FeatureIdentifier implements Cloneable, Serializable {
    public int id;
    public Object name;
    public int stateId;
    FeatureIdentifier() {
    }
    FeatureIdentifier(int fid, int s, Object n) {
	init(fid,s,n);
    }
    FeatureIdentifier(String strRep) {
	StringTokenizer strTok = new StringTokenizer(strRep, ":");
	name = strTok.nextToken();
	id = Integer.parseInt(strTok.nextToken());
	stateId = Integer.parseInt(strTok.nextToken());
    }
    public void init(int fid, int s, Object n) {
	name = n;
	id = fid;
	stateId = s;
    }
    public void init(int fid) {
	id = fid;
    }
    public void copy(FeatureIdentifier fid) {
	init(fid.id,fid.stateId,fid.name);
    }
    public int hashCode() {
	return id;
    }
    public boolean equals(Object o) {
	return (id == ((FeatureIdentifier)o).id);
    }
    public String toString() {
	return name.toString() + ":"  + id+ ":" + stateId;
    }
    public Object clone() {
	return new FeatureIdentifier(id,stateId,name);
    }
};

