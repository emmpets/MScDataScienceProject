package sz.cluster.spark.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;

public class HadoopLinePairIterator implements Iterator<LinePair>, Iterable<LinePair> {

	
	
	private String[] lines1;
	private String[] lines2;
	int i=0,y=0;

	enum state{FILE1,FILE2,CROSS};
	state curstate=state.FILE1;
	
	public HadoopLinePairIterator(HadoopFilePair p) {
		
		try {
			

			 lines1 = read(p.p1).split("\n");
			 lines2 = read(p.p2).split("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
y=i+1;
	}

	private String read(LocatedFileStatus p1) throws IOException { 
		
		StringBuilder sb=new StringBuilder();
		
		BufferedReader br;
	
			br = new BufferedReader(new InputStreamReader(FileSystem.get(DiversityAnalyzerMixStream.getContext().hadoopConfiguration()).open(p1.getPath())));
			 String line;
			    line=br.readLine();
			    while (line != null){
			    	sb.append(line);
			    	sb.append("\n");
			            line=br.readLine();
			            
			    }
		
		
		
		return sb.toString();
   }

	@Override
	public boolean hasNext() {
		switch(curstate)
		{
		/*
		case FILE1:{
			if(i>=lines1.length-1) return false;
		}break;
		case FILE2:{
			if(i>=lines2.length-1) return false;
		}break;
		*/
		case CROSS:{
			if(i>=lines1.length-1&& y>=lines2.length) return false;
		}break;
		default:
		}
		return true;
	}

	@Override
	public LinePair next() {
		LinePair ret=null;
		switch(curstate)
		{
		case FILE1:{
			
			 
			if(y>=lines1.length)
			{
				i++;
				y=i+1;
			}
			if(i>=lines1.length || y>=lines1.length)
			{
				i=0; y=i+1; curstate=state.FILE2;
				return next();
			}
			ret = new LinePair(lines1[i], lines1[y]);
		}break;
		case FILE2:{
			 
			if(y>=lines2.length)
			{
				i++;
				y=i+1;
			}
			if(i>=lines2.length || y>=lines1.length)
			{
				i=0;  y=i; curstate=state.CROSS;
				return next();
			}
			ret = new LinePair(lines2[i], lines2[y]);
		}break;
		case CROSS:{
			 
			 if(y>=lines1.length)
				{
					i++; y=0;
					return next();
				}
			 ret = new LinePair(lines1[i], lines2[y]);
		}break;
		}
		y++;
		return ret;
	
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Iterator<LinePair> iterator() {
		// TODO Auto-generated method stub
		return this;
	}

}
