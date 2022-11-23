package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.beans.PostingList;
import it.unipi.dii.aide.mircv.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.utils.Utility;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Class that implements the merge of the intermediate posting lists during the SPIMI-Indexing algorithm
 */
public class Merger {

    /**
     * Inverted index's next free memory offset
     * */
    private static long memOffset = 0;

    /**
     * Standard pathname for the intermediate index files
     */
    private static final String INTERMEDIATE_INDEX_PATH = ConfigurationParameters.getPartialIndexPath();

    /**
     * number of open indexes: when reaches 0 the indexes are all processed, and we have done
     */
    private static int openIndexes;

    /**
     * number of intermediate indexes produced by SPIMI algorithm
     * */
    private static final int NUM_INTERMEDIATE_INDEXES = Utility.getNumIndexes();

    /**
     * vocabulary path to mapped db
     * */
    private static final String PATH_TO_VOCABULARY = ConfigurationParameters.geVocabularyPath();

    /**
     * memory mapped intermediate indexes
     * */
    private static final Map<Integer, ArrayList<PostingList>> intermediateIndexes = new HashMap<>();

    /**
     * vocabulary for the final inverted index
     * */
    private static ArrayList<VocabularyEntry> vocabulary;

    /**
     * Method that initializes all the data structures:
     * - setting openIndexes to the total number of indexes produced by SPIMI
     * - initializing all the intermediate inverted index structures
     */
    private static void initialize(DB dbInd, DB dbVoc) {
        openIndexes = NUM_INTERMEDIATE_INDEXES;

        // open the vocabulary
        vocabulary = (ArrayList<VocabularyEntry>) dbVoc.indexTreeList("vocabulary", Serializer.JAVA).createOrOpen();

        // get all the intermediate indexes
        for(int i = 0; i < openIndexes; i++) {
            intermediateIndexes.put(i, (ArrayList<PostingList>) dbInd.indexTreeList("index_" + i, Serializer.JAVA).createOrOpen());
        }
    }

    /**
     * Return the minimum term of the terms to be processed in the intermediate indexes
     * @return the next term to process
     */
    private static String getMinTerm(){
        String term = null;

        for(int i = 0; i< NUM_INTERMEDIATE_INDEXES; i++){
            // check if there are still posting lists to be processed at intermediate index 'i'
            if(intermediateIndexes.get(i)==null || intermediateIndexes.get(i).get(0) == null)
                continue;

            // next term to be processed at the intermediate index 'i'
            String nextTerm =  intermediateIndexes.get(i).get(0).getTerm();

            if(term == null){
                term = nextTerm;
                continue;
            }

            if(nextTerm.compareTo(term)<0)
                term = nextTerm;
        }
        return term;
    }

    /**
     * method to process a term in a parallelized way across all the intermediate indexes:
     * - create the final posting list
     * - create the vocabulary entry for the term
     * - update term statistics in the vocabulary entry
     * @param term: term to be processed
     * @return posting list of the processed term
     */
    private static PostingList processTerm(String term){
        // new posting list for the term
        PostingList finalList = new PostingList();
        finalList.setTerm(term);

        // new vocabulary entry for the term
        VocabularyEntry vocabularyEntry = new VocabularyEntry(term);

        // total space occupancy in bytes of the final posting list
        long numBytes = 0;

        // processing the term
        for(int i = 0; i < NUM_INTERMEDIATE_INDEXES; i++){

            // Found the matching term
            if(intermediateIndexes.get(i) != null && intermediateIndexes.get(i).get(0) != null && intermediateIndexes.get(i).get(0).getTerm().equals(term)){

                // intermediate posting list for term 'termToProcess' in index 'i'
                PostingList intermediatePostingList = intermediateIndexes.get(i).get(0);

                // compute memory occupancy and add it to total space occupancy
                numBytes += intermediatePostingList.getNumBytes();

                // Append the posting list to the final posting list of the term
                finalList.appendPostings(intermediatePostingList.getPostings());

                //update vocabulary statistics
                vocabularyEntry.updateStatistics(intermediatePostingList);

                // remove the posting list since it has been processed
                intermediateIndexes.get(i).remove(0);

                // check if index 'i' is empty
                if(intermediateIndexes.get(i).get(0) == null){
                    // set index 'i' to null since it is empty
                    intermediateIndexes.replace(i, null);

                    // decrease the number of open indexes
                    openIndexes--;
                }
            }
        }

        // writing to vocabulary the space occupancy and memory offset of the posting list into
        vocabularyEntry.setMemorySize(numBytes);
        vocabularyEntry.setMemoryOffset(memOffset);

        //compute the final idf
        vocabularyEntry.computeIDF();

        // add vocabulary entry to vocabulary
        vocabulary.add(vocabularyEntry);

        return finalList;
    }

    /**
     * The effective merging pipeline:
     * - finds the minimum term between the indexes
     * - creates the whole posting list and the vocabulary entry for that term
     * - stores them in memory
     * @return true if the merging is complete, false otherwise
     */
    public static boolean mergeIndexes(){

        try(DB dbVoc = DBMaker.fileDB(PATH_TO_VOCABULARY).fileChannelEnable().fileMmapEnable().make(); // vocabulary memory mapped file
            DB dbInd = DBMaker.fileDB(INTERMEDIATE_INDEX_PATH).fileChannelEnable().fileMmapEnable().make() // intermediate indexes memory mapped file
        ){

            // initialization operations
            initialize(dbInd, dbVoc);

            // open all the indexes in parallel and start merging their posting lists
            while(openIndexes > 0){

                // find next term to be processed (the minimum in lexicographical order)
                String termToProcess = getMinTerm();

                // merge the posting lists for the term to be processed
                PostingList mergedPostingList = processTerm(termToProcess);

                // save it on disk and compute the information to store in vocabulary
                int memorySize = mergedPostingList.saveToDisk(memOffset);

                memOffset += memorySize;

            }
            return true;
        } catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}
