package sz.cluster.hadoop.test;

import java.util.Iterator;
import java.util.List;

public class StreamPairwisePermute implements Iterator<StringPair>, Iterable<StringPair> {

	Iterator<String> directoryStream1 = null;
	Iterator<String> directoryStream2 = null;

	int y = 1;

	String pnext = null;
	boolean hasnext;

	private List<String> input2;

	public StreamPairwisePermute(List<String> input1,List<String> input2) {

		try {
		this.input2=input2;
			directoryStream1 = input1.iterator();
			directoryStream2 = input2.iterator();
			
			if (hasnext = directoryStream1.hasNext()) {
				pnext = directoryStream1.next();
				resetS2();
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void resetS2() {

		directoryStream2 = input2.iterator();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		boolean next = false;

		next = directoryStream1.hasNext() || (!directoryStream1.hasNext()&&directoryStream2.hasNext());

		return next;
	}

	@Override
	public StringPair next() {

		StringPair ret = null;
		String p2 = null;
		
		if(!directoryStream2.hasNext())
		{
			pnext=directoryStream1.next();
			resetS2();
		}
		
		
		
		String p1 = pnext;
		p2 = directoryStream2.next();
		 ret = new StringPair(p1, p2);
		

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
