package it.unipi.dii.aide.mircv.common.beans;

public class Posting {

    private int docid;

    private int frequency;

    public Posting(){}

    public Posting(int docid, int frequency){
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
