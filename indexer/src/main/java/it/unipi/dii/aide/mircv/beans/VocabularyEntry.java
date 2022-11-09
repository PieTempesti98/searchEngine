package it.unipi.dii.aide.mircv.beans;

public class VocabularyEntry {
    private static int termCount = 0;

    private int termid;

    private String term;

    private double idf;

    private int tf;

    private long memoryOffset;

    private long memorySize;

    public void updateStatistics(PostingList list){
        //TODO: implement the method [Pietro]
    }


    public void computeMemoryOffsets() {
        //TODO: implement the method [Francesca]
    }

    public void saveToDisk() {
        //TODO: implement the method [Francesca]
    }
}
