package sz.cluster.spark.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import scala.Serializable;

public  class AvgCount implements Serializable, WritableComparable<AvgCount> {
	public AvgCount(double total, int num) {
		total_ = total;
		num_ = num;
	}

	public double total_;
	public int num_;

	public double avg() {
		return total_ / num_;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "total:"+total_+" num:"+num_+" avg:"+avg();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(total_);
		out.writeInt(num_);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		total_=in.readDouble();
		num_=in.readInt();
		
	}
	@Override
	public int compareTo(AvgCount o) {
		// TODO Auto-generated method stub
		return Double.compare(total_, o.total_);
	}
}