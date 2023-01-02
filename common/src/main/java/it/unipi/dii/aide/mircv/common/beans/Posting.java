package it.unipi.dii.aide.mircv.common.beans;

/**
 * a posting in a posting list
 */
public class Posting {

    /**
     * the docid of the posting
     */
    private int docid;

    /**
     * the term frequency of the posting
     */
    private int frequency;

    /**
     * default constructor
     */
    public Posting() {
    }

    /**
     * Constructor that takes all the infromation about the posting
     *
     * @param docid     the docid of the posting
     * @param frequency the term frequency of the posting
     */
    public Posting(int docid, int frequency) {
        this.docid = docid;
        this.frequency = frequency;
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
