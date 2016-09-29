package sz.cluster.hadoop.test;
//package org.apache.lucene.analysis.snowball;

//

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;

import sz.cluster.spark.test.AllPairsFilesSparkIterator;
import sz.cluster.spark.test.HadoopFilePair;
import sz.cluster.spark.test.HadoopLinePairIterator;
import sz.cluster.spark.test.LinePair;
import sz.cluster.spark.test.LinePairIterator;

public class PairMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private final static LongWritable one = new LongWritable(1);
	int id = 0;

	public PairMapper() {
		super();
		Random r = new Random(new Date().getTime());
		id = r.nextInt(1000);
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) {

		// System.out.println("ffsdfsdf\n\n\n\n\n\n"+value+"\n\n\n\n\n");

		try {
			RemoteIterator<LocatedFileStatus> fs = FileSystem.get(context.getConfiguration())
					.listFiles(new Path(value.toString()), false);

			while (fs.hasNext()) {
				LocatedFileStatus ne = fs.next();
				List<String> linearscan = Arrays.asList(read(ne, context).split("\n"));
				
				  StreamSelfPermute aap2=new StreamSelfPermute(linearscan);
				  
				  while(aap2.hasNext()) {
				 
				  StringPair n=aap2.next();
			//	  System.out.println(n);
				writePair(n,context);
				  }
				 
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

		HadoopPermuteFiles pit = new HadoopPermuteFiles(value.toString(), context);

		Hashtable<String, String> grupper = new Hashtable<>();

		while (pit.hasNext()) {
			HadoopFilePair nf = pit.next();
			List<String> l1;
			try {
				l1 = Arrays.asList(read(nf.getP1(), context).split("\n"));
				List<String> l2 = Arrays.asList(read(nf.getP2(), context).split("\n"));
				 StreamPairwisePermute spp=new StreamPairwisePermute(l1, l2);
				 
				 while(spp.hasNext()) {
				 
				 writePair(spp.next(),context); 
				 }
				 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	private void writePair(StringPair stringPair, Mapper<LongWritable, Text, LongWritable, Text>.Context context) {

		Text ret = new Text(stringPair.getS1() + "\n" + stringPair.getS2());

		try {
			context.write(one, ret);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String read(LocatedFileStatus p1, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException {

		StringBuilder sb = new StringBuilder();
		CompressorInputStream input;
		try {
			input = new CompressorStreamFactory().createCompressorInputStream(
					new BufferedInputStream(FileSystem.get(context.getConfiguration()).open(p1.getPath())));

			BufferedReader br;
	

			br = new BufferedReader(new InputStreamReader(input));
			String line;
			line = br.readLine();
			while ((line = br.readLine()) != null) {
				// System.out.println(line);
				if (line.trim().length() == 0){
					continue;
				}
				if (sb.length() > 0) {
					sb.append("\n");
				}
				sb.append(line.trim());

			}
			br.close();
			// System.out.println(sb);
		} catch (CompressorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb.toString();
	}
}