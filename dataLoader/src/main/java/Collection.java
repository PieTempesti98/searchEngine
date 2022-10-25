import java.util.ArrayList;

/**
 * Represents a collection of TextDocuments.
 * The collection can be printed and saved to disk
 */
public class Collection {

    /**
     * The ArrayList of documents of the  collection.
     */
    private ArrayList<TextDocument> documents;

    /**
     * Creates the collection initializing the ArrayList
     */
    public Collection() {
        this.documents = new ArrayList<>();
    }

    /**
     * Add a document to the collection
     * @param doc the TextDocument to add to the collection
     */
    public void addDocument(TextDocument doc) {
        documents.add(doc);
    }

    /**
     * get the arrayList of the TextDocuments forming the collection
     * @return the ArrayList of TextDocuments
     */
    public ArrayList<TextDocument> getDocuments() {
        return documents;
    }

    /**
     * Update the whole collection using the given list
     * @param documents The list of documents to set as the collection
     */
    public void setDocuments(ArrayList<TextDocument> documents) {
        this.documents = documents;
    }

    /**
     * Outputs a string with the collection's information
     * @return a string with the defined information
     */
    public String toString() {
        //TODO select what kind of information we wanna print and the format.
        return "";
    }

    /**
     * Print all the TextDocuments in the collection
     */
    public void printCollection() {
        for (TextDocument doc : this.documents) {
            System.out.println(doc);
        }
    }
}
