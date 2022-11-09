package it.unipi.dii.aide.mircv.common.utils;

/**
 * Class to maintain the collection statistics during the application lifespan
 */
public class CollectionStatistics {

    /**
     * Number of documents in the collection
     */
    private static int numDocuments = 0;

    public static int getNumDocuments() {
        return numDocuments;
    }

    public static void setNumDocuments(int numDocuments) {
        CollectionStatistics.numDocuments = numDocuments;
    }

    /**
     * Add a document to the number of documents
     */
    public static void addDocument(){
        numDocuments++;
    }
}
