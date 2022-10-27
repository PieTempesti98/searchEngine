import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    /**
     * Save the collection to disk
     */
    public void writeToFile(String path) {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        try {
            File outputFile = new File(path);
            if (outputFile.createNewFile()) {
                System.out.println("File created: " + outputFile.getName());
            } else {
                System.out.println("File already exists.");
            }
            for (TextDocument doc : this.documents)
                Files.writeString(Paths.get(path), doc.getPid() + '\t' + doc.getText() + '\n', StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("Working Directory = " + System.getProperty("user.dir"));
            System.out.println("An error occurred while saving data to file.");
            e.printStackTrace();
        }
    }

}
