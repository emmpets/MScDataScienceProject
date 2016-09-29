package sz.cluster.hadoop.test;
//package org.apache.lucene.analysis.snowball;

//
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
//import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
//import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
//import org.apache.commons.compress.compressors.lzma.LZMACompressorInputStream;
//import org.apache.commons.compress.compressors.lzma.LZMAUtils;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.mapreduce.Mapper;

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
				if (ne.getPath().getName().startsWith(".")) continue;
				List<String> linearscan = Arrays.asList(read(ne, context).split("\n"));
				
				  StreamSelfPermute aap2=new StreamSelfPermute(linearscan);
				  
				  while(aap2.hasNext()) {
				 
				  StringPair n=aap2.next();
				  
				  
				
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
			HadoopFilePair currenFilePair = pit.next();
			if (currenFilePair.getP1().getPath().getName().startsWith(".") || currenFilePair.getP2().getPath().getName().startsWith(".")) continue;
			List<String> l1;
			try {
				l1 = Arrays.asList(read(currenFilePair.getP1(), context).split("\n"));
				List<String> l2 = Arrays.asList(read(currenFilePair.getP2(), context).split("\n"));
				 StreamPairwisePermute spp=new StreamPairwisePermute(l1, l2);
				 
				 while(spp.hasNext()) {
					 StringPair p = spp.next();
				
				 writePair(p,context); 
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
	
	private Reader openFile(String name) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(name), 8192);
		if (name.endsWith(".gz")) {
			try {
				is = new GZIPInputStream(is);
			} catch (IOException e) {
				throw new IOException( "Could not read " + name + " as a gz compressed file", e);
			}
		} else if (name.endsWith(".bz2")) {
			try {
				//is.read(); is.read();
				is = new CBZip2InputStream(is);
			} catch (IOException e) {
				throw new IOException( "Could not read " + name + " as a bz2 compressed file", e);
			}
		} 
		
		return new InputStreamReader(is, Charset.forName("UTF-8"));
	} 

	private String read(LocatedFileStatus p1, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException {

		StringBuilder sb = new StringBuilder();
		CompressorInputStream input;
		try {
			input = new CompressorStreamFactory().createCompressorInputStream(
					new BufferedInputStream(FileSystem.get(context.getConfiguration()).open(p1.getPath())));
			//String fileName = "./inputdir/" + p1.getPath().getName();
			//Reader rd = this.openFile(fileName);

			BufferedReader br = new BufferedReader(new InputStreamReader(input));
			//br = new BufferedReader(rd);
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
			//input = new BZip2CompressorInputStream(new BufferedInputStream(FileSystem.get(context.getConfiguration()).open(p1.getPath())));
		}
		return sb.toString();
	}
}