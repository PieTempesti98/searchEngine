package loader;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Loader {

    private static final String PATH_TO_COLLECTION = "data/collection.tsv";
    public static void main(String[] args){
        loadData().printCollection();
    }

     private static Collection loadData(){
        Collection collection = new Collection();
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
