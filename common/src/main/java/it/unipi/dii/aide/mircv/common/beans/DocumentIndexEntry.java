package it.unipi.dii.aide.mircv.common.beans;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;


public class DocumentIndexEntry implements Serializable {

    /**
     * pid of a document
     * */
    String pid;

    /**
    * docid of a document
    * */
    int docid;

    /**
     * length of a documents in terms of number of terms
     * */
    int docLen;

    /**
     * Constructor for the document index entry of a specific document
     * @param pid the pid of such document
     * @param docid the docid of such documents
     * @param docLen the length of such documents in terms of number of terms
     */
    public DocumentIndexEntry(String pid, int docid, int docLen) {
        this.pid = pid;
        this.docid = docid;
        this.docLen = docLen;
    }

    public String getPid() {return pid;}

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getDocid() {
        return docid;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public int getDocLen() {
        return docLen;
    }

    public void setDocLen(int docLen) {
        this.docLen = docLen;
    }

    @Serial
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {

        stream.writeInt(docid);
        stream.writeUTF(pid);
        stream.writeInt(docLen);
    }

    @Serial
    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        docid = stream.readInt();
        pid = stream.readUTF();
        docLen = stream.readInt();

    }

    @Override
    public String toString() {
        return "DocumentIndexEntry{" +
                "pid='" + pid + '\'' +
                ", docid=" + docid +
                ", docLen=" + docLen +
                '}';
    }
}
