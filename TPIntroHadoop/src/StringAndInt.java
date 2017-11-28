import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

class StringAndInt implements Comparable<StringAndInt>, Writable{
	Text tag ;
	IntWritable occurance ;
	
	
	
	

	public StringAndInt() {
		this.occurance = new IntWritable();
		this.tag = new Text();
	}




	public Text getTag() {
		return tag;
	}




	public void setTag(Text tag) {
		this.tag = tag;
	}




	public IntWritable getNbOccurance() {
		return occurance;
	}




	public void setNbOccurance(IntWritable nbOccurance) {
		this.occurance = nbOccurance;
	}




	public StringAndInt(Text tag, IntWritable nbOccurance) {
		
		this.tag = tag;
		this.occurance = nbOccurance;
	}




	@Override
	public String toString() {
		return tag.toString();
	}




	@Override
	public int compareTo(StringAndInt arg) {
		return arg.occurance.compareTo(this.occurance);
	
	}



	@Override
	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		occurance.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		tag.write(out);
		occurance.write(out);
	}

		
	}