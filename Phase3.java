import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;


public class Phase3 {
	//the mapper write to context every couple lexemes with vectors (to be comperd) from gold-dataSet
	public static class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String,ArrayList<String>> goldStandardTable;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			//hashTable for the all the comperd lexemes
			this.goldStandardTable = new HashMap<String, ArrayList<String>>();          
            BufferedReader br = new BufferedReader(new FileReader(new File("D:\\gold-standard.txt")));
            String line;
            while ((line = br.readLine()) != null) {
                String[] lineAfterSplit = line.split("\t");
                //if the "left" word from the GS doesnt exist already in the table, we create the list associated with it and enter them both to the table
                if (!this.goldStandardTable.containsKey(lineAfterSplit[0])){
                	ArrayList<String> arrayList = new ArrayList<String>();
                	arrayList.add(lineAfterSplit[1]);
                	this.goldStandardTable.put(lineAfterSplit[0], arrayList);
                }
                else{
                	ArrayList<String> arrayList = this.goldStandardTable.get(lineAfterSplit[0]);
                	arrayList.add(lineAfterSplit[1]);
                	this.goldStandardTable.put(lineAfterSplit[0], arrayList);
                }
            }
            br.close();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			//extract the lexeme and its vector from the line in the input
			String[] tabSplit = value.toString().split("\t");
			//get the list of the words our lexeme should be compared with
			ArrayList<String> relatedWords = this.goldStandardTable.get(tabSplit[0]);
			if (relatedWords == null){//in case the word our mapper is handling is not in the left side of gold standard
				return;
			}
			//create a json key for that lexeme
			JSONObject jsonKey = new JSONObject();
			jsonKey.put("**vector**", new JSONObject(tabSplit[1]));
			jsonKey.put("**lexeme**", tabSplit[0]);
			Text newKey = new Text(jsonKey.toString());
			//open the merged output file of phase 2 containing all the lexemes and their feature vector
			Path pt = new Path("hdfs://localhost:50071/dsp3output/output2.txt");
            FileSystem fs = null;
			try {
				fs = FileSystem.get(new URI("hdfs://localhost:50071"), context.getConfiguration());
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//iterate the file looking for words that appear in the relatedWords list and write them as values to the context
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tabSplit2 = line.split("\t");
                if (relatedWords.contains(tabSplit2[0])){
                	JSONObject jsonValue = new JSONObject();
        			jsonValue.put("**vector**", new JSONObject(tabSplit2[1]));
        			jsonValue.put("**lexeme**", tabSplit2[0]);
        			context.write(newKey, new Text(jsonValue.toString()));
                }
            }
            br.close();
		}
	}
	
	//the reducer compute vectors distance by diffrent methodes
	public static class Reducer3 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
			for (Text wordVector : values){
				JSONObject rightWord = new JSONObject(wordVector.toString());
				JSONObject leftWord = new JSONObject(key.toString());
				JSONObject pairVector = computeSimilarity(leftWord, rightWord);
				context.write(new Text(leftWord.get("**lexeme**").toString() + "_" + rightWord.get("**lexeme**").toString()), new Text(pairVector.toString())) ;
			}
			
		}
				
				

		private JSONObject computeSimilarity(JSONObject leftWord, JSONObject rightWord) {
			JSONObject ans = new JSONObject();
			JSONObject manResult = manhattanDist(leftWord, rightWord);
			ans.put("manhatten_freq", manResult.get("manhatten_freq"));
			ans.put("manhatten_ttest", manResult.get("manhatten_ttest"));
			ans.put("manhatten_pmi", manResult.get("manhatten_pmi"));
			ans.put("manhatten_prob", manResult.get("manhatten_prob"));
			JSONObject eucResult = euclideanDist(leftWord, rightWord);
			ans.put("euclidean_freq", eucResult.get("euclidean_freq"));
			ans.put("euclidean_ttest", eucResult.get("euclidean_ttest"));
			ans.put("euclidean_pmi", eucResult.get("euclidean_pmi"));
			ans.put("euclidean_prob", eucResult.get("euclidean_prob"));
			JSONObject cosResult = cosineDist(leftWord, rightWord);
			ans.put("cosine_freq", cosResult.get("cosine_freq"));
			ans.put("cosine_ttest", cosResult.get("cosine_ttest"));
			ans.put("cosine_pmi", cosResult.get("cosine_pmi"));
			ans.put("cosine_prob", cosResult.get("cosine_prob"));
			JSONObject jaccardResult = jaccardDist(leftWord, rightWord);
			ans.put("jaccard_freq", jaccardResult.get("jaccard_freq"));
			ans.put("jaccard_ttest", jaccardResult.get("jaccard_ttest"));
			ans.put("jaccard_pmi", jaccardResult.get("jaccard_pmi"));
			ans.put("jaccard_prob", jaccardResult.get("jaccard_prob"));
			JSONObject diceResult = diceDist(leftWord, rightWord);
			ans.put("dice_freq", diceResult.get("dice_freq"));
			ans.put("dice_ttest", diceResult.get("dice_ttest"));
			ans.put("dice_pmi", diceResult.get("dice_pmi"));
			ans.put("dice_prob", diceResult.get("dice_prob"));
			return ans;
			
		}
		
		private JSONObject manhattanDist(JSONObject v1, JSONObject v2){
			JSONObject vector1 = v1.getJSONObject("**vector**");
			double freq=0, t_test=0, pmi=0,prob=0;
			for (String feature : vector1.keySet()){
				if (v2.getJSONObject("**vector**").has(feature)){
					freq += Math.abs(vector1.getJSONObject(feature).getDouble("FREQ") - v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("FREQ"));
					t_test += Math.abs(vector1.getJSONObject(feature).getDouble("TTEST") - v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("TTEST"));
					pmi += Math.abs(vector1.getJSONObject(feature).getDouble("PMI") - v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("PMI"));
					prob += Math.abs(vector1.getJSONObject(feature).getDouble("PROB") - v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("PROB"));
				}
				else {
					freq += Math.abs(vector1.getJSONObject(feature).getDouble("FREQ"));
					t_test += Math.abs(vector1.getJSONObject(feature).getDouble("TTEST"));
					pmi += Math.abs(vector1.getJSONObject(feature).getDouble("PMI"));
					prob +=	Math.abs(vector1.getJSONObject(feature).getDouble("PROB"));	
				}
			}
			JSONObject vector2 = v2.getJSONObject("**vector**");
			for (String feature : vector2.keySet()){
				if (!v1.getJSONObject("**vector**").has(feature)){
					freq += Math.abs(vector2.getJSONObject(feature).getDouble("FREQ"));
					t_test += Math.abs(vector2.getJSONObject(feature).getDouble("TTEST"));
					pmi += Math.abs(vector2.getJSONObject(feature).getDouble("PMI"));
					prob +=	Math.abs(vector2.getJSONObject(feature).getDouble("PROB"));	
				}
			}
			
			
			JSONObject result =  new JSONObject();
			result.put("manhatten_freq", freq);
			result.put("manhatten_ttest", t_test);
			result.put("manhatten_pmi", pmi);
			result.put("manhatten_prob", prob);
			return result;
		}
		
		private JSONObject euclideanDist(JSONObject v1, JSONObject v2){
			JSONObject vector1 = v1.getJSONObject("**vector**");
			double freq=0, t_test=0, pmi=0, prob=0;
			for (String feature : vector1.keySet()){
				if (v2.getJSONObject("**vector**").has(feature)){
					freq += Math.pow(vector1.getJSONObject(feature).getDouble("FREQ") - v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("FREQ"), 2);
					t_test += Math.pow(vector1.getJSONObject(feature).getDouble("TTEST") - v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("TTEST"), 2);
					pmi += Math.pow(vector1.getJSONObject(feature).getDouble("PMI") - v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("PMI"), 2);
					prob += Math.pow(vector1.getJSONObject(feature).getDouble("PROB") - v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("PROB"), 2);
				}
				else {
					freq += Math.pow(vector1.getJSONObject(feature).getDouble("FREQ"), 2);
					t_test += Math.pow(vector1.getJSONObject(feature).getDouble("TTEST"), 2);
					pmi += Math.pow(vector1.getJSONObject(feature).getDouble("PMI"), 2);
					prob += Math.pow(vector1.getJSONObject(feature).getDouble("PROB"), 2);
				}
			}
			JSONObject vector2 = v2.getJSONObject("**vector**");
			for (String feature : vector2.keySet()){
				if (!v1.getJSONObject("**vector**").has(feature)){
					freq += Math.pow(vector2.getJSONObject(feature).getDouble("FREQ"), 2);
					t_test += Math.pow(vector2.getJSONObject(feature).getDouble("TTEST"), 2);
					pmi += Math.pow(vector2.getJSONObject(feature).getDouble("PMI"), 2);
					prob += Math.pow(vector2.getJSONObject(feature).getDouble("PROB"), 2);
				}
			}
			JSONObject result =  new JSONObject();
			result.put("euclidean_freq", Math.sqrt(freq));
			result.put("euclidean_ttest", Math.sqrt(t_test));
			result.put("euclidean_pmi", Math.sqrt(pmi));
			result.put("euclidean_prob", Math.sqrt(prob));
			return result;
	}
		
		private JSONObject cosineDist(JSONObject v1, JSONObject v2){
			JSONObject vector1 = v1.getJSONObject("**vector**");
			double freq=0, t_test=0, v1sum_freq=0, v1sum_ttest=0, v2sum_freq=0, v2sum_ttest=0, pmi=0, prob=0, v1sum_pmi=0, v1sum_prob=0, v2sum_pmi=0, v2sum_prob=0;
			for (String feature : vector1.keySet()){
				if (v2.getJSONObject("**vector**").has(feature)){
					freq += (vector1.getJSONObject(feature).getDouble("FREQ") * v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("FREQ"));
					t_test += (vector1.getJSONObject(feature).getDouble("TTEST") * v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("TTEST"));
					pmi += (vector1.getJSONObject(feature).getDouble("PMI") * v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("PMI"));
					prob += (vector1.getJSONObject(feature).getDouble("PROB") * v2.getJSONObject("**vector**").getJSONObject(feature).getDouble("PROB"));
				}
				v1sum_freq += Math.pow(vector1.getJSONObject(feature).getDouble("FREQ"), 2);
				v1sum_ttest += Math.pow(vector1.getJSONObject(feature).getDouble("TTEST"), 2);
				v1sum_pmi += Math.pow(vector1.getJSONObject(feature).getDouble("PMI"), 2);
				v1sum_prob += Math.pow(vector1.getJSONObject(feature).getDouble("PROB"), 2);
			}
			JSONObject vector2 = v2.getJSONObject("**vector**");
			for (String feature : vector2.keySet()){
				v2sum_freq += Math.pow(vector2.getJSONObject(feature).getDouble("FREQ"), 2);
				v2sum_ttest += Math.pow(vector2.getJSONObject(feature).getDouble("TTEST"), 2);
				v2sum_pmi += Math.pow(vector2.getJSONObject(feature).getDouble("PMI"), 2);
				v2sum_prob += Math.pow(vector2.getJSONObject(feature).getDouble("PROB"), 2);
			}
			
			v1sum_freq = Math.sqrt(v1sum_freq);
			v1sum_ttest = Math.sqrt(v1sum_ttest);
			v2sum_freq = Math.sqrt(v2sum_freq);
			v2sum_ttest = Math.sqrt(v2sum_ttest);
			v1sum_pmi = Math.sqrt(v1sum_pmi);
			v1sum_prob = Math.sqrt(v1sum_prob);
			v2sum_pmi = Math.sqrt(v2sum_pmi);
			v2sum_prob = Math.sqrt(v2sum_prob);
			
			JSONObject result =  new JSONObject();
			result.put("cosine_freq", freq / (v1sum_freq*v2sum_freq));
			result.put("cosine_ttest", t_test / (v1sum_ttest*v2sum_ttest));
			result.put("cosine_pmi", pmi / (v1sum_pmi*v2sum_pmi));
			result.put("cosine_prob", prob / (v1sum_prob*v2sum_prob));
			return result;
		}
		
		private JSONObject jaccardDist(JSONObject v1, JSONObject v2){
		
			JSONObject vector1 = v1.getJSONObject("**vector**");
			JSONObject vector2 = v2.getJSONObject("**vector**");
			double freqMin=0,freqMax=0, t_testMin=0, t_testMax=0, pmiMin=0, pmiMax=0, probMin=0, probMax=0;
			for (String feature : vector1.keySet()){
				if (v2.getJSONObject("**vector**").has(feature)){
					freqMin += Math.min(vector1.getJSONObject(feature).getDouble("FREQ"),vector2.getJSONObject(feature).getDouble("FREQ") );
					t_testMin += Math.min(vector1.getJSONObject(feature).getDouble("TTEST"),vector2.getJSONObject(feature).getDouble("TTEST") );
					freqMax += Math.max(vector1.getJSONObject(feature).getDouble("FREQ"),vector2.getJSONObject(feature).getDouble("FREQ") );
					t_testMax += Math.max(vector1.getJSONObject(feature).getDouble("TTEST"),vector2.getJSONObject(feature).getDouble("TTEST") );	
					pmiMin += Math.min(vector1.getJSONObject(feature).getDouble("PMI"),vector2.getJSONObject(feature).getDouble("PMI") );
					probMin += Math.min(vector1.getJSONObject(feature).getDouble("PROB"),vector2.getJSONObject(feature).getDouble("PROB") );
					pmiMax += Math.max(vector1.getJSONObject(feature).getDouble("PMI"),vector2.getJSONObject(feature).getDouble("PMI") );
					probMax += Math.max(vector1.getJSONObject(feature).getDouble("PROB"),vector2.getJSONObject(feature).getDouble("PROB") );	
				}
				else {
					freqMax += vector1.getJSONObject(feature).getDouble("FREQ");
					t_testMax += vector1.getJSONObject(feature).getDouble("TTEST");
					pmiMax += vector1.getJSONObject(feature).getDouble("PMI");
					probMax +=	vector1.getJSONObject(feature).getDouble("PROB");	
				}
			}
			for (String feature : vector2.keySet()){
				if (!v1.getJSONObject("**vector**").has(feature)){
					freqMax += vector2.getJSONObject(feature).getDouble("FREQ");
					t_testMax += vector2.getJSONObject(feature).getDouble("TTEST");
					pmiMax += vector2.getJSONObject(feature).getDouble("PMI");
					probMax += vector2.getJSONObject(feature).getDouble("PROB");
				}
			}
			
			
			JSONObject result =  new JSONObject();
			result.put("jaccard_freq", freqMin/freqMax);
			result.put("jaccard_ttest", t_testMin/t_testMax);
			result.put("jaccard_pmi", pmiMin/pmiMax);
			result.put("jaccard_prob", probMin/probMax);
			return result;
		}
		
		private JSONObject diceDist(JSONObject v1, JSONObject v2){
			JSONObject vector1 = v1.getJSONObject("**vector**");
			JSONObject vector2 = v2.getJSONObject("**vector**");
			double freqMin=0, v1sum_freq=0, v2sum_freq=0, t_testMin=0, v1sum_ttest=0, v2sum_ttest=0, pmiMin=0, v1sum_pmi=0, v2sum_pmi=0, probMin=0, v1sum_prob=0, v2sum_prob=0;
			for (String feature : vector1.keySet()){
				if (v2.getJSONObject("**vector**").has(feature)){
					freqMin += Math.min(vector1.getJSONObject(feature).getDouble("FREQ"),vector2.getJSONObject(feature).getDouble("FREQ") );
					t_testMin += Math.min(vector1.getJSONObject(feature).getDouble("TTEST"),vector2.getJSONObject(feature).getDouble("TTEST") );
					pmiMin += Math.min(vector1.getJSONObject(feature).getDouble("PMI"),vector2.getJSONObject(feature).getDouble("PMI") );
					probMin += Math.min(vector1.getJSONObject(feature).getDouble("PROB"),vector2.getJSONObject(feature).getDouble("PROB") );
				}
				v1sum_freq += vector1.getJSONObject(feature).getDouble("FREQ");
				v1sum_ttest += vector1.getJSONObject(feature).getDouble("TTEST");
				v1sum_pmi += vector1.getJSONObject(feature).getDouble("PMI");
				v1sum_prob += vector1.getJSONObject(feature).getDouble("PROB");
			}
			for (String feature : vector2.keySet()){
				v2sum_freq += vector2.getJSONObject(feature).getDouble("FREQ");
				v2sum_ttest += vector2.getJSONObject(feature).getDouble("TTEST");
				v2sum_pmi += vector2.getJSONObject(feature).getDouble("PMI");
				v2sum_prob += vector2.getJSONObject(feature).getDouble("PROB");
			}
			
			
			JSONObject result =  new JSONObject();
			result.put("dice_freq", 2*freqMin/(v1sum_freq+v2sum_freq));
			result.put("dice_ttest", 2*t_testMin/(v2sum_ttest+v1sum_ttest));
			result.put("dice_pmi", 2*pmiMin/(v1sum_pmi+v2sum_pmi));
			result.put("dice_prob", 2*probMin/(v2sum_prob+v1sum_prob));
			return result;
		}
		
	}
	
}
