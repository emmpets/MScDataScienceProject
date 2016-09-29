package sz.cluster.spark.test;

import java.util.Arrays;
import java.util.Iterator;

public class TextIterator implements Iterable<String> {

	private String s;
	public  TextIterator(String s) {
		this.s=s;
	}
	@Override
	public Iterator<String> iterator() {
		// TODO Auto-generated method stub
		return Arrays.asList(s.split("\n")).iterator();
	}

}
