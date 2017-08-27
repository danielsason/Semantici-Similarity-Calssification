import java.util.LinkedList;
import java.util.List;

public class WordNode {
	

	    public String getWord() {
	        return word;
	    }

	    private String word;
	    private String pos_tag; // part of speech
	    private String dep_label; // stanford dependency
	    private int father;
	    private List<WordNode> children;
	    private String word_deplabel; //combined word and dep label for use as feature 

	    WordNode(String[] args) {
	        this.word = args[0];
	        this.pos_tag = args[1];
	        this.dep_label = args[2];
	        StringBuilder sb = new StringBuilder();
	        sb.append(args[0]);
	        sb.append("-");
	        sb.append(args[2]);
	        this.word_deplabel = sb.toString(); 
	        try {
	            this.father = Integer.parseInt(args[3]);
	        } catch (Exception e) {
	        }
	        children = new LinkedList<>();
	    }
	    
	    String getWord_DepLabel(){
	    	return word_deplabel;
	    }

	    void addChild(WordNode child) {
	        children.add(child);
	    }

	    int getFather() {
	        return father;
	    }

	    String getDepencdencyPathComponent() {
	        return pos_tag;
	    }

	    List<WordNode> getChildren() {
	        return children;
	    }

}
