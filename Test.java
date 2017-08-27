import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.json.JSONObject;

public class Test {
 
	public static void main(String[] args) throws NumberFormatException, IOException{
		
		HashMap<String, Integer> corpusAppearanceCountTable = new HashMap<>();
		String line; 
		BufferedReader br2 = new BufferedReader(new FileReader(new File("D:\\feature_lexeme_count.txt")));
         while ((line = br2.readLine()) != null) {
             if(line.equals("")){
            	 break;
             }
        	 String[] lineAfterSplit = line.split("\t");
             corpusAppearanceCountTable.put(lineAfterSplit[0], Integer.parseInt(lineAfterSplit[1]));
//             if (lineAfterSplit[0].contains("-")){//if its a feature we put it anyway
//            	 this.corpusAppearanceCountTable.put(lineAfterSplit[0], Integer.parseInt(lineAfterSplit[1]));
//             } else if (this.goldStandardTable.containsKey(lineAfterSplit[0])){//only if the lexeme is in the gold standard we take it
//            	 this.corpusAppearanceCountTable.put(lineAfterSplit[0], Integer.parseInt(lineAfterSplit[1]));
//             }
         }
         br2.close();
         
         BufferedReader br = new BufferedReader(new FileReader(new File("D:\\out1.txt")));
         while ((line = br.readLine()) != null){
        	 HashMap<String, Integer> frequencyTable = new HashMap<>();//this map is for counting how many times a feature appears in the context of a specific lexeme(the key)
			 String[] tabSplit = line.split("\t");
			 String[] splitFeatures = tabSplit[1].split("$");
//			 ArrayList<String> featureList = new ArrayList<>();
			 for (String feature : splitFeatures){
				 if (frequencyTable.containsKey(feature)){
					 frequencyTable.put(feature, frequencyTable.get(feature)+1);
				 }
				 else {
					 frequencyTable.put(feature, 1);
//					 featureList.add(feature);
				 }
			 }
			 
			 double countl = corpusAppearanceCountTable.get(tabSplit[0]);
			 
			 JSONObject finalValue = new JSONObject(); 
			 for (String feature : splitFeatures){
				 if (feature == null){
					 break;
				 }
				 if (!finalValue.has(feature)){
					 JSONObject featureValues = new JSONObject();
					 Integer countf = corpusAppearanceCountTable.get(feature);
					
					 // freq
					 featureValues.put("FREQ", frequencyTable.get(feature));
					 featureValues.put("PROB", (frequencyTable.get(feature)/countl));
					 finalValue.put(feature, featureValues );
				 }
				 
			 }
			
			 
			 System.out.println(finalValue.toString());
         }
         
         
         
	}
}
