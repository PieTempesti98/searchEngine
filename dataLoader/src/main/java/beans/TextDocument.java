package beans;

/**
 * The basic beans.TextDocument, formed by an identifier (pid) and the text payload.
 */
public class TextDocument {

    /**
     * the identifier of the document
     */
    private int pid;

    /**
     * The text payload of the document, encoded in UTF-8
     */
    private String text;

    /**
     * Creates a new beans.TextDocument with the given identifier and payload
     * @param pid the document's identifier
     * @param text the document's payload
     */
    public TextDocument(int pid, String text) {
        this.pid = pid;
        this.text = text;
    }

    /**
     * gets the identifier
     * @return the identifier of the document
     */
    public int getPid() {
        return pid;
    }

    /**
     * sets the pid
     * @param pid the identifier of the document
     */
    public void setPid(int pid) {
        this.pid = pid;
    }

    /**
     * gets the document's text
     * @return the text payload of the document
     */
    public String getText() {
        return text;
    }

    /**
     * sets the document's text
     * @param text the document's payload
     */
    public void setText(String text) {
        this.text = text;
    }

    /**
     * Formats the document as a '[pid] \t [text] \n' string
     * @return the formatted string
     */
    public String toString() {
        return Integer.toString(pid) + '\t' + text + '\n';
    }
}
