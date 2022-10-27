package preprocess;


import ca.rmen.porterstemmer.PorterStemmer;
import beans.TextCollection;
import loader.DataLoader;
import beans.TextDocument;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Locale;



public class Preprocesser {

    private static final String URL_MATCHER = "[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)";
    private static final String HTML_TAGS_MATCHER = "<[^>]+>";
    private static final String NON_DIGIT_MATCHER = "[^a-zA-Z\s]";
    private static final String MULTIPLE_SPACE_MATCHER = "\s+";
    private static final String PATH_TO_STOPWORDS = "data/stopwords.txt";
    private static final String PATH_TO_OUTPUT_FILE = "data/terms.txt";
    private static ArrayList<String> stopwords = new ArrayList<>();
    private static PorterStemmer stemmer = new PorterStemmer();



    private static void readStopwords(){

        try(BufferedReader br = Files.newBufferedReader(Paths.get(PATH_TO_STOPWORDS), StandardCharsets.UTF_8)){
            for(String line; (line = br.readLine()) != null; ){
                // if the line is empty we process the next line
                if(line.isEmpty())
                    continue;

                //add word to stopwords list
                stopwords.add(line);
            }
        } catch(Exception e){
            e.printStackTrace();
        }

    }

    /**
     *  @param text:text to tokenize
     *
     *  performs tokenization and returns the tokens
     * */
    public static String[] tokenize(String text){

        return text.split("\s");
    }

    /**
    * @param text: text to lower
    *
    * returns text in lower case
    *
    * */
    public static String lowerText(String text){
        return text.toLowerCase(Locale.ROOT);
    }

    /**
     *  @param text: text to clean
     *
     *  performs text cleaning
     * */
    public static String cleanText(String text){

        //remove extra spaces at the beginning and end of the text
        text = text.trim();

        //remove urls, if any
        text = text.replaceAll(URL_MATCHER, " ");

        //remove html tags, if any
        text = text.replaceAll(HTML_TAGS_MATCHER," ");

        //remove non-digit characters including punctuation
        text = text.replaceAll(NON_DIGIT_MATCHER,"");

        //remove consecutive multiple whitespaces with a single one
        text = text.replaceAll(MULTIPLE_SPACE_MATCHER," ");

        return text;
    }

    /**
     *
     * @param tokens: tokens to check
     *
*        Check whether there are stopwords in a set
     *                   of tokens and if that's the case, removes them
     *
     * */
    public static String[] removeStopwords(String[] tokens){

        //holder for tokens who are not stopwords
        ArrayList<String> usefulTokens = new ArrayList<>();

        for(String token: tokens)
            if(!stopwords.contains(token)) //if token is not a stopword , keep it
                usefulTokens.add(token);

        return usefulTokens.toArray(new String[0]);
    }

    /**
     *
     * @param tokens: tokens to stem
     *
     * Returns stem of each token
     *
     * */
    public static String[] getStems(String[] tokens){

        //replace each word with its stem
        for(int i = 0; i < tokens.length; i++)
            tokens[i] = stemmer.stemWord(tokens[i]);

        return tokens;
    }



    public static void main(String[] args) {

        readStopwords();

        TextCollection collection = DataLoader.loadData() ;

        //TODO use DTO
        try(FileWriter file = new FileWriter(PATH_TO_OUTPUT_FILE,true)){

            for(TextDocument doc: collection.getDocuments())
            {
                String text = doc.getText();

                //case folding
                text = lowerText(text);

                //text cleaning
                text = cleanText(text);

                // tokenize
                String[] tokens = tokenize(text);

                //remove stopwords
                tokens = removeStopwords(tokens);

                //perform stemming
                tokens = getStems(tokens);

                //TODO write tokens into json file with DTO

                JSONObject jsonObject = new JSONObject();
                //Inserting key-value pairs into the json object
                jsonObject.put("docpid", doc.getPid());

                JSONArray terms= new JSONArray();

                for(int i = 0; i < tokens.length; i++)
                    terms.add(tokens[i]);


                jsonObject.put("terms",terms);

                //write object into file
                file.write(jsonObject.toJSONString() + "\n");

            }
        }  catch (IOException e) {

            e.printStackTrace();
        }

    }
}
