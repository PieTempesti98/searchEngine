package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that represent a document index; it is a singleton hashmap with the docid as key
 */
public class DocumentIndex extends HashMap<Integer, DocumentIndexEntry> {

    /**
     * Instance of the singleton object
     */
    private static DocumentIndex instance = null;

    /**
     * Method used to instantiate the singleton object
     * @return the singleton object
     */
    public static DocumentIndex getInstance(){
        if(instance == null){
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
