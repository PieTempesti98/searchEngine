package it.unipi.dii.aide.mircv.beans;

import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static it.unipi.dii.aide.mircv.common.utils.FileUtils.createIfNotExists;

/**
 * Entry of the vocabulary for a term
 */
public class VocabularyEntry implements Serializable {

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

    private static final String PATH_TO_VOCABULARY = ConfigurationParameters.getVocabularyPath();

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
    public void computeIDF(){
        this.idf = Math.log10(CollectionStatistics.getNumDocuments()/(double)this.df);
    }

    /**
     * Returns the vocabulary entry as a string formatted in the following way:
     * [termid]-[term]-[idf] [tf] [memoryOffset] [memorySize]\n
     * @return the formatted string
     */
    public String toString(){
        //format the string for the vocabulary entry
        String str =    termid + "-" + term + "-" +
                        idf + " " +
                        tf + " " +
                        memoryOffset + " " +
                        memorySize +
                        '\n';
        return str;
    }

    public void setMemorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    public void setMemoryOffset(long memoryOffset) {this.memoryOffset = memoryOffset;
    }
    @Serial
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {

        stream.writeInt(termid);
        stream.writeUTF(term);
        stream.writeInt(tf);
        stream.writeInt(df);
        stream.writeDouble(idf);
        stream.writeLong(memoryOffset);
        stream.writeLong(memorySize);
    }

    @Serial
    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        termid = stream.readInt();
        term = stream.readUTF();
        tf = stream.readInt();
        df = stream.readInt();
        idf = stream.readDouble();
        memoryOffset = stream.readLong();
        memorySize = stream.readLong();

    }

}
