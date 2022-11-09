package it.unipi.dii.aide.mircv.beans;

import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;

import java.util.Map;

/**
 * Entry of the vocabulary for a term
 */
public class VocabularyEntry {

    /**
     * incremental counter of the terms, used to assign the termid
     */
    private static int termCount = 0;

    /**
     * termid of the specific term
     */
    private int termid;

    /**
     * Term to which refers the vocabulary entry
     */
    private String term;

    /**
     * Document frequency of the term
     */
    private int df = 0;

    /**
     * term frequency of the term in the whole collection
     */
    private int tf = 0;

    /**
     * inverse of document frequency of the term
     */
    private double idf = 0;

    /**
     * starting point of the term's posting list in the inverted index in bytes
     */
    private long memoryOffset = 0;

    /**
     * size of the term's posting list in the inverted index in bytes
     */
    private long memorySize = 0;

    /**
     * Constructor for the vocabulary entry for the term passed as parameter
     * Assign the termid to the term and initializes all the statistics and memory information
     * @param term the token of the entry
     */
    public VocabularyEntry(String term){

        // Assign the term
        this.term = term;

        // Assign the termid and increase the counter
        this.termid = termCount;
        termCount ++;
    }

    /**
     * Updates the statistics of the vocabulary:
     * updates tf and df with the data of the partial posting list processed
     * @param list the posting list from which the method computes the statistics
     */
    public void updateStatistics(PostingList list){

        //for each element of the intermediate posting list
        for(Map.Entry<Integer, Integer> posting: list.getPostings()){

            // update the term frequency
            this.tf += posting.getValue();

            // update the raw document frequency
            this.df++;
        }
    }

    /**
     * Compute the idf using the values computed during the merging of the indexes
     */
    private void computeIDF(){
        this.idf = Math.log10(CollectionStatistics.getNumDocuments()/(double)this.df);
    }

    public void computeMemoryOffsets() {
        //TODO: implement the method [Francesca]
    }

    public void saveToDisk() {
        //TODO: implement the method [Francesca]
    }
}
