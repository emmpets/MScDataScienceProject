package sz.cluster.hadoop.test;

public class StringPair {
	String s1, s2;

	public StringPair(String p1, String p2) {
		super();
		this.s1 = p1;
		this.s2 = p2;
	}

	public String getS1() {
		return s1;
	}

	public String getS2() {
		return s2;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "("+s1+"),("+s2+")";
	}
}
