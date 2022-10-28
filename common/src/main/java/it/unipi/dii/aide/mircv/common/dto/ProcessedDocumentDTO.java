package it.unipi.dii.aide.mircv.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO object for the processed class
 */
public class ProcessedDocumentDTO {

    /**
     * Document PID
     */
    @JsonProperty("pid")
    private int pid;

    /**
     * Array with all the processed terms
     */
    @JsonProperty("tokens")
    private String[] tokens;

    /**
     * Constructor of a document
     * @param pid PID of the document
     * @param tokens array with the processed tokens
     */
    public ProcessedDocumentDTO(int pid, String[] tokens) {
        this.pid = pid;
        this.tokens = tokens;
    }

    /**
     * 0-parameter constructor: instantiates the document
     */
    public ProcessedDocumentDTO() {
    }

    /**
     * @return the document PID
     */
    public int getPid() {
        return pid;
    }

    /**
     * @param pid the PID to set
     */
    public void setPid(int pid) {
        this.pid = pid;
    }

    /**
     * @return the array of the tokens
     */
    public String[] getTokens() {
        return tokens;
    }

    /**
     * @param tokens the tokens to set for the document
     */
    public void setTokens(String[] tokens) {
        this.tokens = tokens;
    }
}
