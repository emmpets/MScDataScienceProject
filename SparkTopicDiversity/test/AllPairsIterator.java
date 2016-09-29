package sz.cluster.spark.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class AllPairsIterator implements Iterator<FilePair>, Iterable<FilePair> {

	Iterator<Path> directoryStream1 = null;
	Iterator<Path> directoryStream2 = null;
	private String uritofiledir;
	int y = 1;

	Path pnext = null;
	boolean hasnext;

	public AllPairsIterator(String uritofiledir) {
		this.uritofiledir = uritofiledir;
		try {
			directoryStream1 = Files.newDirectoryStream(Paths.get(uritofiledir)).iterator();

			if (hasnext = directoryStream1.hasNext()) {
				pnext = directoryStream1.next();
				hasnext = directoryStream1.hasNext();
			}

		} catch (IOException ex) {
		}
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub

		return hasnext;
	}

	@Override
	public FilePair next() {

		Path p2 = null;
		Path p1 = pnext;
		if (directoryStream2 == null || !directoryStream2.hasNext()) {
			try {
				directoryStream2 = Files.newDirectoryStream(Paths.get(uritofiledir)).iterator();
			} catch (IOException e) {
				e.printStackTrace();
			}

			for (int y = this.y; y > 0; y--) {
				directoryStream2.next();
			}

			this.y++;
			System.out.println(this.y);

			pnext = directoryStream1.next();
		}
		p2 = directoryStream2.next();

		FilePair ret = new FilePair(p1, p2);

		if (!directoryStream1.hasNext()) {
			hasnext = false;
		}
		return ret;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator<FilePair> iterator() {
		// TODO Auto-generated method stub
		return this;
	}

}
