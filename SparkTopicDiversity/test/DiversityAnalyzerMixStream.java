package sz.cluster.spark.test;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class DiversityAnalyzerMixStream {

	public static class AvgCount implements java.io.Serializable {
		public AvgCount(double total, int num) {
			total_ = total;
			num_ = num;
		}

		public double total_;
		public int num_;

		public double avg() {
			return total_ / num_;
		}
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return "total:"+total_+" num:"+num_+" avg:"+avg();
		}
	}

public static JavaSparkContext getContext() {
	return context;
}
	
	private static final FlatMapFunction<String, String> LINE_EXTRACTOR = new FlatMapFunction<String, String>() {
		@Override
		public Iterable<String> call(String s) throws Exception {
			return new TextIterator(s);
		}
	};

	private static final FlatMapFunction<String, FilePair> FILE_PAIR_CREATOR = new FlatMapFunction<String, FilePair>() {

		@Override
		public Iterable<FilePair> call(String t) throws Exception {
			// TODO Auto-generated method stub
			return new AllPairsIterator(t);
		}
	};
	
	private static final FlatMapFunction<String, HadoopFilePair> HADOOP_FILE_PAIR_CREATOR = new FlatMapFunction<String, HadoopFilePair>() {

		@Override
		public Iterable<HadoopFilePair> call(String t) throws Exception {
			// TODO Auto-generated method stub
			return new AllPairsFilesSparkIterator(t);
		}
	};
	
	private static final FlatMapFunction<FilePair, LinePair> LINE_PAIR_CREATOR = new FlatMapFunction<FilePair, LinePair>() {

		@Override
		public Iterable<LinePair> call(FilePair p) throws Exception {
			System.out.println("Create lineIterator for pair: "+p.p1.getFileName()+" and "+p.p2.getFileName());
			return new LinePairIterator(p);
		}
	};
	private static final FlatMapFunction<HadoopFilePair, LinePair> HADOOP_LINE_PAIR_CREATOR = new FlatMapFunction<HadoopFilePair, LinePair>() {

		@Override
		public Iterable<LinePair> call(HadoopFilePair p) throws Exception {
			System.out.println("Create lineIterator for pair: "+p.p1+" and "+p.p2);
			return new HadoopLinePairIterator(p);
		}
	};
	private static final PairFunction<FilePair, Integer, Double> FILE_DIVERSITY_COMPUTER = new PairFunction<FilePair, Integer, Double>() {
		@Override
		public Tuple2<Integer, Double> call(FilePair s) throws Exception {

			String content1 = new String(Files.readAllBytes(s.p1));
			String content2 = new String(Files.readAllBytes(s.p2));

			System.out.println("compute diversity for files: "+s.p1.getFileName()+" and "+s.p2.getFileName());
			double val = AllPairs.jaccardSimilarity(parseAndStemm(content1), parseAndStemm(content2));

			return new Tuple2<Integer, Double>(1, val);
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

	};
	
	private static final PairFunction<LinePair, Integer, Double> LINE_DIVERSITY_COMPUTER = new PairFunction<LinePair, Integer, Double>() {
		@Override
		public Tuple2<Integer, Double> call(LinePair s) throws Exception {

			String content1 = s.line1;
			String content2 = s.line2;
			

			System.out.println("compute diversity for lines: "+s.line1+" and "+s.line2);
			double val = AllPairs.jaccardSimilarity(parseAndStemm(content1), parseAndStemm(content2));

			return new Tuple2<Integer, Double>(1, val);
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

	};

	private static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {
		@Override
		public Integer call(Integer a, Integer b) throws Exception {
			return a + b;
		}
	};
	
	
	
	private static final Function<Double, AvgCount> AVERAGE_EXPANDER = new Function<Double, AvgCount>() {
		@Override
		public AvgCount call(Double x) {
			return new AvgCount(x, 1);
		}
	};

	private static final Function2<AvgCount, Double, AvgCount> AVERAGER_addAndCount = new Function2<AvgCount, Double, AvgCount>() {
		@Override
		public AvgCount call(AvgCount a, Double x) {
			Random r=new Random();
			try {
				if(true)
				Thread.sleep(r.nextInt(4000));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("sum up");
			a.total_ += x;
			a.num_ += 1;
			return a;
		}
	};
	private static final Function2<AvgCount, AvgCount, AvgCount> AVERAGER_COMBINE = new Function2<AvgCount, AvgCount, AvgCount>() {
		@Override
		public AvgCount call(AvgCount a, AvgCount b) {
			a.total_ += b.total_;
			a.num_ += b.num_;
			return a;
		}
	};

	private static JavaSparkContext context;
	
	

	public static void main(String[] args) {
		args = new String[] { "/media/zerr/BA0E0E3E0E0DF3E3/darkskies/test.arff" };
		if (args.length < 1) {
			System.err.println("Please provide the input file full path as argument");
			System.exit(0);
		}
boolean local=true;
		SparkConf conf = new SparkConf().setAppName("sz.cluster.spark.AllPairs");
		if(local){
		conf=conf.setMaster("local");
		}
	
		 context = new JavaSparkContext(conf);

		//File containing input directories
		JavaRDD<String> file = context.textFile("input.txt");
		
		
		//Read directory name
		JavaRDD<String> words = file.flatMap(LINE_EXTRACTOR);
		
		//create pair iterator combining all possible pairs from a directory
		JavaRDD<HadoopFilePair> fpairs = words.flatMap(HADOOP_FILE_PAIR_CREATOR);
		
		
		
		
		JavaRDD<LinePair> pairs = fpairs.flatMap(HADOOP_LINE_PAIR_CREATOR);
		
		
		
		 SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		    // Create the context with 2 seconds batch size
		    
		    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		    
		  

		//COmpute diversity for each pair
		JavaPairRDD<Integer, Double> diversity = pairs.mapToPair(LINE_DIVERSITY_COMPUTER);

		
//combine all diversity values and average.
		JavaPairRDD<Integer, AvgCount> avgCounts = diversity.combineByKey(AVERAGE_EXPANDER, AVERAGER_addAndCount, AVERAGER_COMBINE);

		Map<Integer, AvgCount> countMap = avgCounts.collectAsMap();
		for (Entry<Integer, AvgCount> entry : countMap.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue().avg());
		}

		System.out.println(avgCounts);
Random r=new Random();
		avgCounts.saveAsTextFile("diversityout"+r.nextInt(1000));

	}

}