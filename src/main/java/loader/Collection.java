package loader;

import java.util.ArrayList;

public class Collection {
    private ArrayList<TextDocument> documents = new ArrayList<>();

    public void addDocument(TextDocument doc){
        documents.add(doc);
    }

    public ArrayList<TextDocument> getDocuments() {
        return documents;
    }

    public void setDocuments(ArrayList<TextDocument> documents) {
        this.documents = documents;
    }

    public String toString(){
        StringBuilder builder = new StringBuilder();
        for(TextDocument doc: this.documents){
            builder.append(doc.toString());
        }
        return builder.toString();
    }

    public void printCollection(){
        for(TextDocument doc: this.documents){
            System.out.println(doc);
        }
    }
}
