package it.unipi.dii.aide.mircv.beans;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;

public class PostingList {

    private String term;
    private final ArrayList<Map.Entry<Integer, Integer>> postings = new ArrayList<>();

    public PostingList(String toParse) {
        String[] termRow = toParse.split("\t");
        this.term = termRow[0];
        parsePostings(termRow[1]);
    }

    public PostingList(){}
    private void parsePostings(String rawPostings){
        String[] documents = rawPostings.split(" ");
        for(String elem: documents){
            String[] posting = elem.split(":");
            postings.add(new AbstractMap.SimpleEntry<>(Integer.parseInt(posting[0]), Integer.parseInt(posting[1])));
        }
    }

    public String getTerm() {
        return term;
    }

    public ArrayList<Map.Entry<Integer, Integer>> getPostings() {
        return postings;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void appendPostings(ArrayList<Map.Entry<Integer, Integer>> newPostings){
        postings.addAll(newPostings);
    }

    public void saveToDisk() {
        //TODO: implement the method [Francesca]
    }
}
