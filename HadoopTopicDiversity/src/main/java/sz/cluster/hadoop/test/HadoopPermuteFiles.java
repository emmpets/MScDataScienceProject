package sz.cluster.hadoop.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopPermuteFiles implements Iterator<HadoopFilePair>, Iterable<HadoopFilePair> {

	RemoteIterator<LocatedFileStatus> directoryStream1 = null;
	RemoteIterator<LocatedFileStatus> directoryStream2 = null;
	private String uritofiledir;
	int y = 1;

	LocatedFileStatus pnext = null;
	boolean hasnext;
	private Mapper<LongWritable, Text, Text, IntWritable>.Context context;

	public HadoopPermuteFiles(String uritofiledir, Mapper.Context context) {
		this.uritofiledir = uritofiledir;
		this.context=context;
		//FileSystem.get(sc.hadoopConfiguration()).listFiles(..., true)
	try {
		
		directoryStream1= FileSystem.get(context.getConfiguration()).listFiles(new Path(uritofiledir), false);
		if (hasnext = directoryStream1.hasNext()) {
			pnext = directoryStream1.next();
			resetS2();
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

	private void resetS2() throws FileNotFoundException, IllegalArgumentException, IOException {
		
		directoryStream2 = FileSystem.get(context.getConfiguration()).listFiles(new Path(uritofiledir), false);
		for (int y = this.y; y > 0; y--) {
			directoryStream2.next();
		}

		this.y++;
		
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
boolean next=false;
try {
	next = directoryStream1.hasNext()&&(directoryStream2==null||directoryStream2.hasNext());
} catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
catch (NullPointerException e) {
	
	e.printStackTrace();
}
		return next;
	}

	@Override
	public HadoopFilePair next(){
	HadoopFilePair ret=null;
		LocatedFileStatus p2 = null;
		LocatedFileStatus p1 = pnext;
		
		
		try {


			 p2=directoryStream2.next();
			 ret = new HadoopFilePair(p1, p2);
				if(!directoryStream2.hasNext())
				{
					pnext=directoryStream1.next();
					resetS2();
				}
			 
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalArgumentException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
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
