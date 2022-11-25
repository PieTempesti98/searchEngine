package it.unipi.dii.aide.mircv.common.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;

/**
 * DTO object to map the processed collection to JSON file
 */
public class ProcessedCollection {

    /**
     * List of the processed documents in the collection
     */
    @JsonProperty("data")
    private ArrayList<ProcessedDocument> data = new ArrayList<>();

    /**
     * Creates the object with the given list of processed documents to map into JSON
     * @param data the list of documents
     */
    public ProcessedCollection(ArrayList<ProcessedDocument> data) {
        this.data = data;
    }

    /**
     * 0-parameter constructor, instantiating the collection with no documents in the list
     */
    public ProcessedCollection() {
    }

    /**
     * @return the list of documents
     */
    public ArrayList<ProcessedDocument> getData() {
        return data;
    }


    /**
     * @param data the list of documents to set as the collection
     */
    public void setData(ArrayList<ProcessedDocument> data) {
        this.data = data;
    }
}
