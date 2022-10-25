package loader;

public class TextDocument {
    private int docid;
    private String text;
    public TextDocument(int docid, String text) {
        this.docid = docid;
        this.text = text;
    }

    public int getDocid() {
        return docid;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String toString(){
        return Integer.toString(docid) + '\t' + text + '\n';
    }
}
