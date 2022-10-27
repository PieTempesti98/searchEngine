package loader;

import beans.TextCollection;
import beans.TextDocument;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * The Main class for the loader: loads the .tsv collection, parses encoding the characters in UTF-8 and
 * saves the parsed collection on disk.
 */
public class DataLoader {

    /**
     * The path to the .tsv collection
     */
    private static final String PATH_TO_COLLECTION = "data/sampled_collection.tsv";

    /**
     * The main method, that loads the collection and so on...
     * @param args the command line arguments passed at the start of the module
     */
    public static void main(String[] args){
        //TODO define what we want to do with the main method and then update the javadoc
        loadData().printCollection();
    }

    /**
     * Loads the data from disk, parses them to UTF-8 and creates the collection of text documents
     * @return the TextDocument collection of parsed documents
     */
    public static TextCollection loadData(){
        TextCollection collection = new TextCollection();
        try(BufferedReader br = Files.newBufferedReader(Paths.get(PATH_TO_COLLECTION), StandardCharsets.UTF_8)){
            for(String line; (line = br.readLine()) != null; ){
                // if the line is empty we process the next line
                if(line.isEmpty())
                    continue;
                // split of the line in the format <pid>\t<text>
                String[] split = line.split("\t");

                // Creation of the text document for the line and insert in the collection
                TextDocument doc = new TextDocument(Integer.parseInt(split[0]), split[1]);
                collection.addDocument(doc);

            }
        } catch(Exception e){
            e.printStackTrace();
        }
        return collection;
    }
}
