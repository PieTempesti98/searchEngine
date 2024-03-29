package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import java.util.LinkedHashMap;

/**
 * Class that represent a document index; it is a singleton hashmap with the docid as key
 */
public class DocumentIndex extends LinkedHashMap<Integer, DocumentIndexEntry> {

    /**
     * Instance of the singleton object
     */
    private static DocumentIndex instance = null;

    /**
     * default constructor
     */
    private DocumentIndex() {
    }

    /**
     * Method used to instantiate the singleton object
     *
     * @return the singleton object
     */
    public static DocumentIndex getInstance() {
        if (instance == null) {
            instance = new DocumentIndex();
        }
        return instance;
    }

    /**
     * Lookup method on the document index
     * @param docid the key
     * @return the pid of the document
     */
    public String getPid(int docid){
        return this.get(docid).getPid();
    }


    /**
     * Lookup method on the document index
     * @param docid the key
     * @return the length of the document
     */
    public int getLength(int docid){return this.get(docid).getDocLen();}

    /**
     * Loads the document index from disk
     * @return true if the fetch is successful
     */
    public boolean loadFromDisk(){
        // retrieve the number of documents
        long numDocuments = CollectionSize.getCollectionSize();

        // retrieve the size of a single entry
        final int ENTRY_SIZE = DocumentIndexEntry.getEntrySize();

        // for each document to be fetched
        for(int i = 0; i < numDocuments; i++){

            // load the document from disk and put it into the map
            DocumentIndexEntry newEntry = new DocumentIndexEntry();
            if(newEntry.readFromDisk((long) i * ENTRY_SIZE)){
                this.put(newEntry.getDocid(), newEntry);
            }
            else{
                return false;
            }
        }
        return true;
    }



}
