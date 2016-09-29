package sz.algorithm.diversity;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;



public class AllPairs 
{


	
	 public static double jaccardSimilarity(Set<String> d1, Set<String> d2)
	    {
	        int overlap = 0;
	        Set<String> shortd;
	        Set<String> longd;
	        if(d1.size() > d2.size())
	        {
	            shortd = d2;
	            longd = d1;
	        } else
	        {
	            shortd = d1;
	            longd = d2;
	        }
	        HashSet<String> union=new HashSet<String>(d1);
	        union.addAll(d2);
	        for(Iterator<String> iterator = shortd.iterator(); iterator.hasNext();)
	        {
	            Object d = iterator.next();
	            if(longd.contains(d))
	                overlap++;
	            
	        }

	        return ((double)overlap * 1.0D) / union.size();
	    }
	 
 public static void main(String[] args) {
	
	 
	 HashSet<String> d1=new HashSet<String>(Arrays.asList("sun,snow,ski,ice".split(",")));
	 HashSet<String> d2=new HashSet<String>(Arrays.asList("sun,snow,ski,ice".split(",")));
	 System.out.println(jaccardSimilarity(d1, d2));
	 
	 
	 HashSet<String> d3=new HashSet<String>(Arrays.asList("sun,snow,ski,ice".split(",")));
	 HashSet<String> d4=new HashSet<String>(Arrays.asList("sun,beach,sea,sand".split(",")));
	 System.out.println(jaccardSimilarity(d3, d4));
	 
	 HashSet<String> d5=new HashSet<String>(Arrays.asList("snow,ski,ice".split(",")));
	 HashSet<String> d6=new HashSet<String>(Arrays.asList("beach,sea,sand".split(",")));
	 System.out.println(jaccardSimilarity(d5, d6));
}
}
