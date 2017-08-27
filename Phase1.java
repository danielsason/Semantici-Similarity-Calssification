import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase1 {
	 //input: Ngrams biracs data
	 // the mapper extract lexemes and features fron ngrams data
	 public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

		 private final String REGEX = "[^a-zA-Z ]+";

 
		 	 /**
	         * Map a key-value pair.
	         * @param key the line index of the current line of the input file.
	         * @param value the line contents.
	         * @param context the Map-Reduce job context.
	         * @throws IOException
	         * @throws InterruptedException
	         */
	        @Override
	        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	            String[] components = value.toString().split("\t");
	            String ngram = components[1];			
	            String[] parts = ngram.split(" ");
	            WordNode[] nodes = getNodes(parts);
	            if (nodes == null)
	                return;
	            WordNode root = constructParsedTree(nodes);
	            writeChildrenToContext(root, context);			// write to context. lexem and related featues. in addition, all the lexemes and features for total counting)
	        }

	        /**
	         * Transforms a biarc into a an array of Nodes
	         * @param parts an array of Strings, each index being a biarc
	         * @return an array of Nodes, each one containing a biarc in an accessible container.
	         */
	        private WordNode[] getNodes(String[] parts) {
	            WordNode[] partsAsNodes = new WordNode[parts.length];
	            for (int i = 0; i < parts.length; i++) {
	                String[] ngramEntryComponents = parts[i].split("/");
	                if (ngramEntryComponents.length != 4)
	                    return null;
	                ngramEntryComponents[0] = ngramEntryComponents[0].replaceAll(REGEX, "");
	                if (ngramEntryComponents[0].replaceAll(REGEX, "").equals(""))
	                    return null;
	                ngramEntryComponents[1] = ngramEntryComponents[1].replaceAll(REGEX, "");
	                if (ngramEntryComponents[1].replaceAll(REGEX, "").equals(""))
	                    return null;
	                partsAsNodes[i] = new WordNode(ngramEntryComponents);
	            }
	            return partsAsNodes;
	        }

	        /**
	         * Transforms an array of Nodes into a tree, which represents the dependencies defined in the original biarc.
	         * @param nodes an array of Nodes.
	         * @return the root of the tree.
	         */
	        private WordNode constructParsedTree(WordNode[] nodes) {
	            int rootIndex = 0;
	            for (int i = 0; i < nodes.length; i++) {
	                if (nodes[i].getFather() > 0)
	                    nodes[nodes[i].getFather() - 1].addChild(nodes[i]);
	                else
	                    rootIndex = i;
	            }
	            return nodes[rootIndex];
	        }

	        /**
	         * A method to combine and write all lexemes and features to reduce method
	         * @param father the root, realted lexem, of the features.
	         * @param context the Map-Reduce job context.
	         * @throws IOException
	         * @throws InterruptedException
	         */
	        private void writeChildrenToContext(WordNode father, Context context) throws IOException, InterruptedException {

	        	Text key = new Text();
	        	StringBuilder sb = new StringBuilder();
	        	sb.append(father.getWord());
	        	sb.append("#");		//sign lexem with '#'
	        	key.set(sb.toString());
        		context.write(key, new Text("1"));
        		for (WordNode child : father.getChildren()){
        			key.set(child.getWord_DepLabel());
	        		context.write(key, new Text("1"));
	        		key.set(father.getWord());
	        		context.write(key, new Text(child.getWord_DepLabel()));
	        	}
	    }
	 }
	 
	 //the reducer calculate totalFeatures, totalLexems, specific feature/lexem apperances and related fetures to appropriate lexeme
	 public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		 
		 enum CountersEnum {LEXEME, FEATURE};
		 private Counter[] counters;
		 private BufferedWriter bw;
		 HashMap<String, Boolean> goldStandardTable;
//		 private Integer numOfFeatures = 0;
		 
		 @Override
		 public void setup(Context context) throws IOException{
		 	 //load the gold dataset to memory to filter unnseccary lexems
			 this.goldStandardTable = new HashMap<String, Boolean>();
			 BufferedReader br = new BufferedReader(new FileReader(new File("D:\\gold-standard.txt")));
             String line;
             while ((line = br.readLine()) != null) {
                 String[] lineAfterSplit = line.split("\t");
                 if (!this.goldStandardTable.containsKey(lineAfterSplit[0])){
                 	this.goldStandardTable.put(lineAfterSplit[0], true);
                 }
                 if (!this.goldStandardTable.containsKey(lineAfterSplit[1])){
                 	this.goldStandardTable.put(lineAfterSplit[1], true);
                 }
             }
             br.close();
			 
			 
			 //two counters for total lexems and features
			 counters = new Counter[2];
			 counters[0] = context.getCounter(CountersEnum.class.getName(), CountersEnum.LEXEME.toString());
			 counters[1] = context.getCounter(CountersEnum.class.getName(), CountersEnum.FEATURE.toString());
			 bw = new BufferedWriter(new FileWriter(new File("D:\\feature_lexeme_count.txt")));
		 
		 }
		 
		 @Override
	     public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 Long sum = 0l;
			 
			 if (key.toString().contains("-") || key.toString().contains("#")) {// in case key= lexem/feature count them
				 for (Text value : values){
					 sum++;			//count specific features/lexemes appearences 
				 } 
				 //write lexeme counter to file
				 if (key.toString().contains("#")){
					 counters[1].increment(sum);
					 bw.write(key.toString().substring(0,key.toString().length()-1) + "\t" + sum.toString());// remove the "#"
					 bw.newLine();
				 }
				 else {		//write feature counter to file
					 counters[0].increment(sum);
					 bw.write(key.toString() + "\t" + sum.toString());
					 bw.newLine();
				 }
				 
			 }
			 else {//this case its a lexeme and all the features in its context, we append all the features and send them as the value and the lexeme as the key
				 if (!this.goldStandardTable.containsKey(key.toString())){
					 return;
				 }
				 StringBuilder sb = new StringBuilder();
				 for (Text value : values){
					 sb.append(value.toString());
					 sb.append(" ");
				 }
				 String append = sb.toString();
				 append = append.substring(0, append.length()-1);
				 context.write(key, new Text(append));				// write to context lexem and related features as chain
			}
		}

		 
		@Override
		public void cleanup(Context context) throws IOException{
			this.bw.close();
		}

			
	 }	
}
