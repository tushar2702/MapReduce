package pagerank;

import java.io.*;
import org.apache.hadoop.io.*;

public class FlightPair implements Writable {

	private Text airport;
	private int count;

	public FlightPair() {
		this.airport = new Text();
	}
	
	public FlightPair(Text airport, int count) {
		this.airport = airport;
		this.count = count;
	}

	public Text getAirport() {
		return airport;
	}

	public int getCount() {
		return count;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		airport.write(out);
		out.writeInt(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		airport.readFields(in);
		count = in.readInt();
	}

	@Override
	public String toString() {
		return airport + "\t" + count;
	}

}
