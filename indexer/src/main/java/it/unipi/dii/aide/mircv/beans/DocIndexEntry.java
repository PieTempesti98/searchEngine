package it.unipi.dii.aide.mircv.beans;

public class DocIndexEntry {
    private int docid;
    private String pid;
    private int doclen;

    public DocIndexEntry(int docid, String pid, int doclen) {
        this.docid = docid;
        this.pid = pid;
        this.doclen = doclen;
    }

    public int getDocid() {
        return docid;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getDoclen() {
        return doclen;
    }

    public void setDoclen(int doclen) {
        this.doclen = doclen;
    }
}
