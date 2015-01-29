package ai.vital.hadoop.writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class VitalBytesWritable implements Writable {

	private byte[] bytes;
	
	public VitalBytesWritable(byte[] bytes) {
		super();
		this.bytes = bytes;
	}
	
	public VitalBytesWritable() {
		super();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if(bytes == null) throw new IOException("Null bytes array");
		out.writeInt(bytes.length);
		out.write(bytes);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		int length = in.readInt();
		bytes = new byte[length];
		in.readFully(bytes);

	}

	public byte[] get() {
		return bytes;
	}

	public void set(byte[] bytes) {
		if( bytes == null ) throw new NullPointerException("Bytes array cannot be empty.");
		this.bytes = bytes;
	}
	
	

}