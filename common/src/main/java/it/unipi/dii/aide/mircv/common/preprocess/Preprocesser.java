package it.unipi.dii.aide.mircv.common.preprocess;

import ca.rmen.porterstemmer.PorterStemmer;
import it.unipi.dii.aide.mircv.common.beans.TextDocument;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

public class Preprocesser {

    /**
     * regEx to match urls
     */
    private static final String URL_MATCHER = "[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)";

    /**
     * regEx to match html tags
     */
    private static final String HTML_TAGS_MATCHER = "<[^>]+>";

    /**
     * regEx to match a character that is not a letter
     */
    private static final String NON_DIGIT_MATCHER = "[^a-zA-Z ]";

    /**
     * regEx to match sequential spaces
     */
    private static final String MULTIPLE_SPACE_MATCHER = " +";

    /**
     * regEx to match at least 3 consecutive letters
     */
    private static final String CONSECUTIVE_LETTERS_MATCHER = "(.)\\1{3,}";

    /**
     * regEx to match strings in camel case
     */
    private static final String CAMEL_CASE_MATCHER = "(?<=[a-z])(?=[A-Z])";

    /**
     * path to file storing stopwords
     */
    private static final String PATH_TO_STOPWORDS = ConfigurationParameters.getStopwordsPath();

    /**
     * list of stopwords
     */
    private static final ArrayList<String> stopwords = new ArrayList<>();

    /**
     * object performing stemming
     */
    private static final PorterStemmer stemmer = new PorterStemmer();

    /**
     * maximum length a term should have
     */
    private static final int TRESHOLD = 64;

    /**
     * reads stopwords from a file and loads them in main memory
     */
    public static void readStopwords(){

        try (BufferedReader br = Files.newBufferedReader(Paths.get(PATH_TO_STOPWORDS), StandardCharsets.UTF_8)) {
            for (String line; (line = br.readLine()) != null; ) {
                // if the line is empty we process the next line
                if (line.isEmpty())
                    continue;

                //add word to stopwords list
                stopwords.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * @param text:text to tokenize
*                  performs tokenization
* @return array of tokens
*
     */
    public static String[] tokenize(String text) {

        //list of tokens
        ArrayList<String> tokens = new ArrayList<>();

        //tokenize splitting on whitespaces
        String[] splittedText = text.split("\s");

        for(String token: splittedText) {
            //split words who are in CamelCase
            String[] subtokens = token.split(CAMEL_CASE_MATCHER);
            for (String subtoken : subtokens) {
                //if a token has a length over a certain threshold, cut it at the threshold value
                subtoken = subtoken.substring(0, Math.min(subtoken.length(), TRESHOLD));
                //return token in lower case
                tokens.add(subtoken.toLowerCase(Locale.ROOT));
            }
        }

        return tokens.toArray(new String[0]);
    }


    /**
     * @param text: text to clean
     *              performs text cleaning
     * @return cleaned text
     */
    public static String cleanText(String text) {


        //remove urls, if any
        text = text.replaceAll(URL_MATCHER, "\s");

        //remove html tags, if any
        text = text.replaceAll(HTML_TAGS_MATCHER, "\s");

        //remove non-digit characters including punctuation
        text = text.replaceAll(NON_DIGIT_MATCHER, "\s");

        //collapse 3+ repeating characters in just 2
        text = text.replaceAll(CONSECUTIVE_LETTERS_MATCHER,"$1$1");

        //remove consecutive multiple whitespaces with a single one
        text = text.replaceAll(MULTIPLE_SPACE_MATCHER, "\s");

        //remove possible spaces and the beginning and the end of the document
        text = text.trim();

        return text;
    }

    /**
     * @param tokens: set of tokens
     * @return tokens without stopwords
     */
    public static String[] removeStopwords(String[] tokens) {

        //holder for tokens who are not stopwords
        ArrayList<String> usefulTokens = new ArrayList<>();

        for (String token : tokens) {
            //if token is not a stopword and its length does not exceed the trashold, keep it
            if (!stopwords.contains(token) && token.length() <= TRESHOLD)
                usefulTokens.add(token);
        }


        return usefulTokens.toArray(new String[0]);
    }

    /**
     * @param tokens: tokens to stem
     *
     * @return Returns stem of each token
     */
    public static String[] getStems(String[] tokens) {

        //replace each word with its stem
        for (int i = 0; i < tokens.length; i++)
            tokens[i] = stemmer.stemWord(tokens[i]);

        return tokens;

    }

    /**
     * Perform the preprocessing of a TextDocument, transforming it in a document formed by
     * its PID and the list of its tokens
     * @param doc the TextDocument to preprocess
     * @return the processed document
     */
    public static ProcessedDocument processDocument(TextDocument doc) {

        String text = doc.getText();

        // text cleaning
        text = cleanText(text);


        // tokenize
        String[] tokens = tokenize(text);

        for(String token: tokens){
            if(token.length() > 64){
                System.out.println(text);
                System.out.println(token);
            }
        }

        // remove stopwords
        tokens = removeStopwords(tokens);

        // perform stemming
        getStems(tokens);

        // Return the processed document
        return new ProcessedDocument(doc.getPid(), tokens);

    }


}

