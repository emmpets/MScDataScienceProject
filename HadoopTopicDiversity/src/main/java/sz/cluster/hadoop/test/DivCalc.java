package sz.cluster.hadoop.test;
//package org.apache.lucene.analysis.snowball;
//

import java.io.IOException;
import java.io.StringReader;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;

import sz.algorithm.diversity.AllPairs;

public class DivCalc extends Reducer<LongWritable, Text, LongWritable, Text> {

    private final static LongWritable one = new LongWritable(1);
	private Integer id=null;


public DivCalc() {
	super();
	
}

@Override
protected void reduce(LongWritable arg0, Iterable<Text> value,
		Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {

	 if(id==null){Random r=new Random(new Date().getTime());
		id=r.nextInt(1000);}
	 Iterator<Text> it = value.iterator();
    	while(it.hasNext())
    	{
    		Text n = it.next();
    		try {
    			//Text n = it.next();
      		  System.out.println(n);
      		  System.out.println();
      		String lines[]=n.toString().split("\n");
      		double val = AllPairs.jaccardSimilarity(parseAndStemm(lines[0]), parseAndStemm(lines[1]));
      		
      	//	System.out.println("Cоmpute similarity(by "+id+"): "+lines[0]+" and "+lines[1]);
      		context.write(one, new Text(val+""));
    		} catch (ArrayIndexOutOfBoundsException e) {
    			System.out.println("String Failure: " + n.toString());
    		}
    		 	
    	}

}


public HashSet<String> parseAndStemm(String in) {
	HashSet<String> ret = new HashSet<>();
	SnowballAnalyzer al = new SnowballAnalyzer(Version.LUCENE_30, "English", new HashSet<String>());
	StringReader sr = new StringReader(in);
	TokenStream ts = al.tokenStream("", sr);
	try {
		while (ts.incrementToken()) {
			TermAttribute ta = ts.getAttribute(TermAttribute.class);
			ret.add(ta.term());

		}
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	return ret;
}

  

}