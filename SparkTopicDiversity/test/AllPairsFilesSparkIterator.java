package sz.cluster.spark.test;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class AllPairsFilesSparkIterator implements Iterator<HadoopFilePair>, Iterable<HadoopFilePair> {

	RemoteIterator<LocatedFileStatus> directoryStream1 = null;
	RemoteIterator<LocatedFileStatus> directoryStream2 = null;
	private String uritofiledir;
	int y = 1;

	LocatedFileStatus pnext = null;
	boolean hasnext;

	public AllPairsFilesSparkIterator(String uritofiledir) {
		this.uritofiledir = uritofiledir;
		//FileSystem.get(sc.hadoopConfiguration()).listFiles(..., true)
	try {
		directoryStream1= FileSystem.get(DiversityAnalyzerMixStream.getContext().hadoopConfiguration()).listFiles(new Path(uritofiledir), false);
		if (hasnext = directoryStream1.hasNext()) {
			pnext = directoryStream1.next();
			hasnext = directoryStream1.hasNext();
		}
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IllegalArgumentException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub

		return hasnext;
	}

	@Override
	public HadoopFilePair next() {
		HadoopFilePair ret=null;
		LocatedFileStatus p2 = null;
		LocatedFileStatus p1 = pnext;
		try {
	
			if (directoryStream2 == null || !directoryStream2.hasNext()) {
				try {
					
					directoryStream2 = FileSystem.get(DiversityAnalyzerMixStream.getContext().hadoopConfiguration()).listFiles(new Path(uritofiledir), false);
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

			 ret = new HadoopFilePair(p1, p2);

			if (!directoryStream1.hasNext()) {
				hasnext = false;
			}
			
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ret;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator<HadoopFilePair> iterator() {
		// TODO Auto-generated method stub
		return this;
	}

}
