package it.unipi.dii.aide.mircv.common.beans;


import java.util.ArrayList;
import java.util.List;

/**
 * Object representing a processed document
 */
public class ProcessedDocument {

    /**
     * Document PID
     */

    private String pid;

    /**
     * Array with all the processed terms
     */

    private ArrayList<String> tokens;

    /**
     * Constructor of a document
     * @param pid PID of the document
     * @param tokens array with the processed tokens
     */
    public ProcessedDocument(String pid, String[] tokens) {
        this.pid = pid;
        this.tokens = new ArrayList<>(List.of(tokens));
    }

    /**
     * 0-parameter constructor: instantiates the document
     */
    public ProcessedDocument() {
    }

    /**
     * @return the document PID
     */
    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public ArrayList<String> getTokens() {
        return tokens;
    }

    public void setTokens(ArrayList<String> tokens) {
        this.tokens = tokens;
    }

    /**
     * Returns the processed document as a string formatted in the following way:
     * [pid] \t [token1,token2,token3,...,tokenN]
     * @return the formatted string
     */
    public String toString(){
        StringBuilder str = new StringBuilder(pid + "\t");

        if(tokens.size() == 0) {
            str.append('\n');
            return str.toString();
        }

        //Append to the StringBuilder all the tokens than the last, separated by comma
        for(int i = 0; i < tokens.size() - 1; i++){
            str.append(tokens.get(i));
            str.append(',');
        }
        // Append the last element without the comma, then append the newline character
        str.append(tokens.get(tokens.size() - 1));
        str.append('\n');
        return str.toString();
    }
}
