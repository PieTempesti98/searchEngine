package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.beans.PostingList;
import it.unipi.dii.aide.mircv.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.utils.Utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Class that implements the merge of the intermediate posting lists during the SPIMI-Indexing algorithm
 */
public class Merger {

    /**
     * List of the bufferedReaders for each intermediate index
     * If a value is set to null, the index has been fully processed
     */
    private static final ArrayList<BufferedReader> buffers = new ArrayList<>();

    /**
     * List of the next term to process (in lexicographical order) for each intermediate index
     * If a value is set to null, the index has been fully processed
     */
    private static final ArrayList<String> nextTerm = new ArrayList<>();

    /**
     * List of the next posting list to process for each intermediate index
     * If a value is set to null, the index has been fully processed
     */
    private static final ArrayList<PostingList> nextPostingList = new ArrayList<>();

    /**
     * Standard pathname for the intermediate index files
     */
    private static final String INTERMEDIATE_INDEX_PATH = "data/index_";

    /**
     * number of open indexes: when reaches 0 the indexes are all processed, and we have done
     */
    private static int openIndexes;

    // private static final ArrayList<VocabularyEntry> vocabulary = new ArrayList<>();

    /**
     * Method that initializes all the data structures, opening the buffers
     * and initializing the lists ponting to the first term to process in each index
     * @throws Exception exceptions related to the buffer opening and handling
     */
    private static void initialize() throws Exception{
        openIndexes = Utility.getNumIndexes();

        // number of empty buffers
        int emptyIndexes = 0;

        for(int i = 0; i < openIndexes; i++){
            String path = INTERMEDIATE_INDEX_PATH + i + ".txt";
            BufferedReader buffer = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8);
            String line = buffer.readLine();

            // If the buffer is emptu we add null in its position
            if(line == null){
                buffers.add(null);
                nextTerm.add(null);
                nextPostingList.add(null);

                // increase the number of empty indexes
                emptyIndexes++;
                continue;
            }

            PostingList list = new PostingList(line);

            buffers.add(buffer);
            nextTerm.add(list.getTerm());
            nextPostingList.add(list);
        }

        // fix the number of open buffers by removing the null buffers
        openIndexes -= emptyIndexes;
    }

    /**
     * read a new line from a buffer and update the data structures related to the index
     * @param i the number of the intermediate index
     * @throws IOException exception related to the buffer handling and close
     */
    private static void readBufferLine(int i) throws IOException {

        String line = buffers.get(i).readLine();

        // If the buffer is emptu we close it, and we set null in the pointers
        if(line == null){
            buffers.get(i).close();
            buffers.set(i, null);
            nextTerm.set(i, null);
            nextPostingList.set(i, null);

            // decrease the number of open indexes
            openIndexes--;
            return;
        }

        // create the new posting list
        PostingList list = new PostingList(line);

        // update the correct entry of the lists
        nextTerm.set(i, list.getTerm());
        nextPostingList.set(i, list);
    }

    /**
     * Return the minimum term of the nextTerm list in lexicographical order
     * @return the next term to process
     */
    private static String getMinTerm(){
        String term = nextTerm.get(0);
        for(String elem: nextTerm){
            if(elem.compareTo(term) < 0){
                term = elem;
            }
        }
        return term;
    }

    /**
     * The effective merging function:
     * find the minimum term between the indexes
     * creates the whole posting list and the vocabulary entry for that term
     * stores them in memory
     * Update the pointers by scanning the intermediate indexes
     * @return true if the merging is complete, false otherwise
     */
    public static boolean mergeIndexes(){

        try{
            initialize();

            while(openIndexes > 0){
                String termToProcess = getMinTerm();
                PostingList finalList = new PostingList();
                finalList.setTerm(termToProcess);

                VocabularyEntry vocabularyEntry = new VocabularyEntry(termToProcess);
                for(int i = 0; i < Utility.getNumIndexes(); i++){
                    // Found the matching term
                    if(nextTerm.get(i) != null && nextTerm.get(i).equals(termToProcess)){

                        PostingList list = nextPostingList.get(i);
                        // Append the posting list to the final posting list of the term
                        finalList.appendPostings(list.getPostings());

                        //Update the lists for the scanned index
                        readBufferLine(i);

                        //update vocabulary statistics
                        vocabularyEntry.updateStatistics(list);
                    }
                }
                //the list for the term is computed, save it on disk and compute the information to store in vocabulary
                finalList.saveToDisk();

                vocabularyEntry.computeMemoryOffsets();
                vocabularyEntry.saveToDisk();

            }
            return true;
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

}
