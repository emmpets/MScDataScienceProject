package sz.cluster.spark.test;

public class LinePair {
	
	String line1;
	String line2;
	public LinePair(String line1, String line2) {
		super();
		this.line1 = line1;
		this.line2 = line2;
	}
	public String getLine1() {
		return line1;
	}
	public String getLine2() {
		return line2;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "("+line1+"),("+line2+")";
	}
}
