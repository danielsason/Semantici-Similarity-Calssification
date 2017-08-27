import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;



public class Phase2 {
	 //the mapper calculate for every lexem the value of his fetures by diffrent measures
	 public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

		 private Long totalLexemes;
		 private Long totalFeatures;
		 HashMap<String, Integer> corpusAppearanceCountTable;
		 HashMap<String, Boolean> goldStandardTable;
		 
		 
		 @Override
		 public void setup(Context context) throws IOException{
			 this.totalLexemes = context.getConfiguration().getLong("TotalLexemes",0L);
			 this.totalFeatures = context.getConfiguration().getLong("TotalFeatures",0L);

			 //hashMap for lexemes and features appearences
			 this.corpusAppearanceCountTable = new HashMap<String,Integer>();

             //fill up corpus lexeme and features appearance count table from saved file
             BufferedReader br2 = new BufferedReader(new FileReader(new File("D:\\feature_lexeme_count.txt")));
             while ((line = br2.readLine()) != null) {
                 if(line.equals("")){
                	 break;
                 }
            	 String[] lineAfterSplit = line.split("\t");
                 this.corpusAppearanceCountTable.put(lineAfterSplit[0], Integer.parseInt(lineAfterSplit[1]));
             }
             br2.close();
		 }
		 
		 @Override
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			 //this map is for counting how many times a feature appears in the context of a specific lexeme(the key)
			 HashMap<String, Integer> frequencyTable = new HashMap<>();
			
			 String[] tabSplit = value.toString().split("\t");
			 String[] splitFeatures = tabSplit[1].split(" ");

			 //counts apperances of features that related to this lexem
			 for (String feature : splitFeatures){
				 if (frequencyTable.containsKey(feature)){
					 frequencyTable.put(feature, frequencyTable.get(feature)+1);
				 }
				 else {
					 frequencyTable.put(feature, 1);
				 }
			 }
			 
			 // that lexem apperances 
			 double countl = corpusAppearanceCountTable.get(tabSplit[0]);
			 

			 //calcuate measures of association with context for every feture  
			 JSONObject finalValue = new JSONObject(); 
			 for (String feature : splitFeatures){
				 if (feature == null){				//edge case 
					 break;
				 }
				 if (!finalValue.has(feature)){		// prevent repetitions
					 JSONObject featureValues = new JSONObject();
					 Integer countf = this.corpusAppearanceCountTable.get(feature);
					 // freq
					 featureValues.put("FREQ", frequencyTable.get(feature));
					 featureValues.put("PROB", (frequencyTable.get(feature)/countl));
					 double pl = ((double)countl/this.totalLexemes);
					 double pf = ((double)countf/this.totalFeatures);
					 double plf = (frequencyTable.get(feature)/countl) * pl;
					 
					 featureValues.put("PMI", log(plf,2) - log(pl*pf, 2));
					 featureValues.put("TTEST", ((plf-(pl*pf))/Math.sqrt(pl*pf)));
				
					 finalValue.put(feature, featureValues);
				 }
				 
			 }
			 
			 //lexem and his present pvector
			 context.write(new Text(tabSplit[0]), new Text(finalValue.toString()));
		 }
		

		 private double log(double x, int base){
			 return (Math.log(x) / Math.log(base));
		 }
	}
}
