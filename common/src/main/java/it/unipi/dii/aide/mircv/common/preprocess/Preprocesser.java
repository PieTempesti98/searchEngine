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
    private static final String NON_DIGIT_MATCHER = "[^a-zA-Z\s]";

    /**
     * regEx to match sequential spaces
     */
    private static final String MULTIPLE_SPACE_MATCHER = "\s+";

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
     * reads stopwords from a file and loads them in main memory
     */
    public static void readStopwords() {

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

        return text.split("\s");
    }

    /**
     * @param text: text to lower
     * @return text in lower case
     */
    public static String lowerText(String text) {
        return text.toLowerCase(Locale.ROOT);
    }

    /**
     * @param text: text to clean
     *              performs text cleaning
     * @return cleaned text
     */
    public static String cleanText(String text) {


        //remove urls, if any
        text = text.replaceAll(URL_MATCHER, " ");

        //remove html tags, if any
        text = text.replaceAll(HTML_TAGS_MATCHER, " ");

        //remove non-digit characters including punctuation
        text = text.replaceAll(NON_DIGIT_MATCHER, " ");

        //remove consecutive multiple whitespaces with a single one
        text = text.replaceAll(MULTIPLE_SPACE_MATCHER, " ");

        //remove extra spaces at the beginning and end of the text
        text = text.trim();

        return text;
    }

    /**
     * @param tokens: tokens to check
     *                Check whether there are stopwords in a set
     *                of tokens and if that's the case, removes them
     * @return tokens without stopwords
     */
    public static String[] removeStopwords(String[] tokens) {

        //holder for tokens who are not stopwords
        ArrayList<String> usefulTokens = new ArrayList<>();

        for (String token : tokens) {
            if (!stopwords.contains(token)) //if token is not a stopword , keep it
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

        // case folding
        text = lowerText(text);

        // text cleaning
        text = cleanText(text);

        // tokenize
        String[] tokens = tokenize(text);

        // remove stopwords
        tokens = removeStopwords(tokens);

        // perform stemming
        getStems(tokens);

        // Return the processed document
        return new ProcessedDocument(doc.getPid(), tokens);

    }


}

