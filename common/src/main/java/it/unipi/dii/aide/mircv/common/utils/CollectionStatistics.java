package it.unipi.dii.aide.mircv.common.utils;

public class CollectionStatistics {
    private static int numDocuments = 0;

    public static int getNumDocuments() {
        return numDocuments;
    }

    public static void setNumDocuments(int numDocuments) {
        CollectionStatistics.numDocuments = numDocuments;
    }

    public static void addDocument(){
        numDocuments++;
    }
}
