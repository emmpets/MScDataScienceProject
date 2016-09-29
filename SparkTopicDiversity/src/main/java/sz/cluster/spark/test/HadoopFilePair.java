package sz.cluster.spark.test;

import org.apache.hadoop.fs.LocatedFileStatus;

public class HadoopFilePair {

LocatedFileStatus p1; LocatedFileStatus p2;



public HadoopFilePair(LocatedFileStatus p1, LocatedFileStatus p2) {
	super();
	this.p1 = p1;
	this.p2 = p2;
}

public LocatedFileStatus getP1() {
	return p1;
}
public LocatedFileStatus getP2() {
	return p2;
}
@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "("+p1.getPath().getName()+"),("+p2.getPath().getName()+")";
	}
}
