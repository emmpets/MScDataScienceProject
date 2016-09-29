package sz.cluster.spark.test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;

public class LinePairIterator implements Iterator<LinePair>, Iterable<LinePair> {

	
	
	private String[] lines1;
	private String[] lines2;
	int i=0,y=0;

	enum state{FILE1,FILE2,CROSS};
	state curstate=state.FILE1;
	
	public LinePairIterator(FilePair p) {
		
		try {
			 lines1 = new String(Files.readAllBytes(p.p1)).split("\n");
			 lines2 = new String(Files.readAllBytes(p.p2)).split("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
y=i+1;
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
