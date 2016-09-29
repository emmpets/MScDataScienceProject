package sz.cluster.hadoop.test;

import java.util.Iterator;
import java.util.List;

public class StreamSelfPermute implements Iterator<StringPair>, Iterable<StringPair> {

	Iterator<String> directoryStream1 = null;
	Iterator<String> directoryStream2 = null;

	int y = 1;

	String pnext = null;
	boolean hasnext;
	private List<String> input;

	public StreamSelfPermute(List<String> input) {

		try {
			this.input = input;
			directoryStream1 = input.iterator();
			if (hasnext = directoryStream1.hasNext()) {
				pnext = directoryStream1.next();
				directoryStream2=input.iterator();
				resetS2();
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void resetS2() {

		directoryStream2 = input.iterator();
		for (int y = this.y; y > 0; y--) {
			directoryStream2.next();
		}

		this.y++;

	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		boolean next = false;

		next = directoryStream1.hasNext() || (directoryStream2.hasNext());

		return next;
	}

	@Override
	public StringPair next() {

		StringPair ret = null;
		String p2 = null;
	

		p2 = directoryStream2.next();
		 ret = new StringPair(pnext, p2);
		if (!directoryStream2.hasNext()) {
			pnext = directoryStream1.next();
			resetS2();
		}

		return ret;

	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator<StringPair> iterator() {
		// TODO Auto-generated method stub
		return this;
	}

}
