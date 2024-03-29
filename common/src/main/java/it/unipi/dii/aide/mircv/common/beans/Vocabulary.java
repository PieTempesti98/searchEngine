package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import org.junit.platform.commons.util.LruCache;
import java.util.LinkedHashMap;

/**
 * The singleton vocabulary object
 */
public class Vocabulary extends LinkedHashMap<String, VocabularyEntry> {

    private static Vocabulary instance = null;
    /**
     * cache used for most recently used vocabulary entries
     */
    private final static LruCache<String, VocabularyEntry> entries= new LruCache<>(1000);

    /**
     * path to file storing the vocabulary
     */
    protected static String VOCABULARY_PATH = ConfigurationParameters.getVocabularyPath();

    private Vocabulary(){}

    /**
     * singleton pattern
     */
    public static Vocabulary getInstance(){
        if(instance == null){
            instance = new Vocabulary();
        }
        return instance;
    }

    /**
     * get idf of a term
     * @param term term of which we wnat to get the idf
     * @return idf of such term
     */
    public double getIdf(String term){
        return getEntry(term).getIdf();
    }

    /**
     * gets vocabulary entry of a given term
     * @param term term of which we want to get its vocabulary entry
     * @return vocabulary entry of such term
     */
    public VocabularyEntry getEntry(String term){

        //if term is cached, return its vocabulary entry
        if(entries.containsKey(term))
            return entries.get(term);

        //get entry from disk
        VocabularyEntry entry = findEntry(term);

        //cache the entry
        if(entry != null)
            entries.put(term,entry);

        return entry;

    }

    /**
     *  used for testing purposes only
     * */
    public boolean readFromDisk(){

        long position = 0;

        //read whole vocabulary from
        while(position >= 0){
            VocabularyEntry entry = new VocabularyEntry();
            //read entry and update position
            position = entry.readFromDisk(position,VOCABULARY_PATH);

            if(position == 0)
                return  true;

            if(entry.getTerm()==null){
                return true;
            }

            //populate vocabulary
            this.put(entry.getTerm(),entry);
            entries.put(entry.getTerm(),entry);

        }

        //if position == -1 an error occurred during reading
        return position != -1;

    }

    /**
     * retrieves the vocabulary entry of a given term from disk
     * @param term: term of which we want vocabulary entry
     * @return the vocabulary entry of given term, null if term is not in vocabulary
     **/
    public VocabularyEntry findEntry(String term){


        VocabularyEntry entry = new VocabularyEntry(); //entry to be returned

        long start = 0; //index of first element of vocabulary portion on which search is performed
        long end = CollectionSize.getVocabularySize() -1; //index of last element of vocabulary portion on which search is performed
        long mid; //index of element of the vocabulary to be read
        String key; //term read from vocabulary
        long entrySize = VocabularyEntry.ENTRY_SIZE; //size of a vocabulary entry


        //performing binary search to get vocabulary entry
        while (start <= end) {

            // find new entry to read
            mid = start + (end - start) / 2;

            //get entry from disk
            entry.readFromDisk(mid * entrySize, VOCABULARY_PATH);
            key = entry.getTerm();

            //check if the search was successful
            if (key.equals(term)) {
                return entry;
            }

            //update search portion parameters
            if (term.compareTo(key) > 0) {
                start = mid + 1;
                continue;
            }

            end = mid - 1;
        }
        return null;
    }

    /** needed for testing purposes
     * @param path: path to be set
     */
    public static void setVocabularyPath(String path) {VOCABULARY_PATH = path;}

    /** needed for testing purposes
     */
    public static void clearCache() {

        entries.clear();
    }

    /**
     * needed for testing purposes
     */
    public static void unsetInstance(){
        instance = null;
    }

}
