package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.CollectionSize;

public class Posting {

    private int docid;

    private int frequency;

    public Posting(){}

    public Posting(int docid, int frequency){
        this.docid = docid;
        this.frequency = frequency;
    }

    /**
     * @param entry vocabulary entry of the term we want to score
     * @return score computed according to BM25
     */

    private double computeBM25(VocabularyEntry entry){

        double k1 = 1.5;
        double b = 0.75;

        //get idf of term
        double idf = entry.getIdf();
        //get frequency of term occurring in document we are scoring
        double tf = (1 + Math.log10(frequency));
        //get document length
        int docLen = DocumentIndex.getInstance().getLength(docid);
        //get average document length
        double avgDocLen = (double) CollectionSize.getTotalDocLen()/CollectionSize.getCollectionSize();

        //return score
        return idf * tf  / ( tf + k1 * (1 - b + b * docLen/avgDocLen));

    }

    /**
     * @param entry vocabulary entry of the term we want to score
     * @return score computed according to tfidf
     */
    private double computeTFIDF(VocabularyEntry entry){

        //get idf of term
        double idf = entry.getIdf();
        //get frequency of term occurring in document we are scoring
        double tf = (1 + Math.log10(frequency));

        //return score
        return idf * tf ;
    }

    public  double scoreDocument(VocabularyEntry entry,String scoringFunction) {

        return ((scoringFunction.equals("bm25"))? computeBM25(entry) : computeTFIDF(entry));
    }

    public int getDocid() {
        return docid;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "Posting{" +
                "docid=" + docid +
                ", frequency=" + frequency +
                '}';
    }
}
