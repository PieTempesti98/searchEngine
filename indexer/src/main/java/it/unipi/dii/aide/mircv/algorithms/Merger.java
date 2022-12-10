package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Class that implements the merge of the intermediate posting lists during the SPIMI-Indexing algorithm
 */
public class Merger {

    /* TODO: take from config parameters; it should be the same value as the one in VocbularyEntry file */
    private static final long VOCETRY_SIZE = 72;
    /**
     * Inverted index's next free memory offset in docids file
     */
    private static long docsMemOffset = 0;

    /**
     * Inverted index's next free memory offset in freqs file
     */
    private static long freqsMemOffset = 0;

    /**
     * number of intermediate indexes produced by SPIMI algorithm
     */
    private static int numIndexes;

    /**
     * Standard pathname for partial index documents files
     */
    private static final String PATH_TO_PARTIAL_INDEXES_DOCS = ConfigurationParameters.getPartialIndexPath();

    /**
     * Standard pathname for partial index frequencies files
     */
    private static final String PATH_TO_PARTIAL_INDEXES_FREQS = ConfigurationParameters.getPartialIndexPath();

    /**
     * Standard pathname for partial vocabulary files
     */
    private static final String PATH_TO_PARTIAL_VOCABULARIES = ConfigurationParameters.getPartialIndexPath();

    /**
     * Path to the inverted index docs file
     */
    private final static String PATH_TO_INVERTED_INDEX_DOCS = ConfigurationParameters.getInvertedIndexDocsPath();

    /**
     * Path to the inverted index freqs file
     */
    private final static String PATH_TO_INVERTED_INDEX_FREQS = ConfigurationParameters.getInvertedIndexFreqsPath();

    /**
     * Path to vocabulary
     */
    private static final String PATH_TO_VOCABULARY = ConfigurationParameters.getVocabularyPath();

    /**
     * Array used to point to the next vocabulary entry to process for each partial index
     */
    private static VocabularyEntry[] nextTerms = null;

    /**
     * memory offsets of last read vocabulary entry
     */
    private static long[] vocEntryMemOffset = null;


    /**
     * Method that initializes all the data structures:
     * - setting openIndexes to the total number of indexes produced by SPIMI
     * - initializing all the intermediate inverted index structures
     */
    private static void initialize() {

        // initialization of array of next vocabulary entries tp be processed
        nextTerms = new VocabularyEntry[numIndexes];

        // initialization of next memory offset to be read for each partial vocabulary
        vocEntryMemOffset = new long[numIndexes];

        for (int i=0; i < numIndexes; i++){

            nextTerms[i] = new VocabularyEntry();
            vocEntryMemOffset[i] = 0;

            // read first entry of the vocabulary
            boolean ret = nextTerms[i].readFromDisk(vocEntryMemOffset[i], PATH_TO_PARTIAL_VOCABULARIES+i);

            if(!ret){
                // error encountered during vocabulary entry reading operation
                nextTerms[i] = null;
            }
        }
    }

    /**
     * Return the minimum term of the terms to be processed in the intermediate indexes
     * @return the next term to process
     */
    private static String getMinTerm() {
        String term = null;

        for (int i = 0; i < numIndexes; i++) {

            // check if there are still posting lists to be processed at intermediate index 'i'
            if (nextTerms[i] == null)
                continue;

            // next term to be processed at the intermediate index 'i'
            String nextTerm = nextTerms[i].getTerm();

            if (term == null) {
                term = nextTerm;
                continue;
            }

            if (nextTerm.compareTo(term) < 0)
                term = nextTerm;
        }
        return term;
    }

    /**
     * method to process a term in a parallelized way across all the intermediate indexes:
     * - create the final posting list
     * - create the vocabulary entry for the term
     * - update term statistics in the vocabulary entry
     *
     * @param termToProcess: term to be processed
     * @param vocabularyEntry: vocabulary entry for new term
     * @return posting list of the processed term
     */
    private static PostingList processTerm(String termToProcess, VocabularyEntry vocabularyEntry) {
        // new posting list for the term
        PostingList finalList = new PostingList();
        finalList.setTerm(termToProcess);

        // total space occupancy in bytes of the final posting list
        long numBytes = 0;

        // processing the term
        for (int i = 0; i < numIndexes; i++) {
            // Found the matching term
            if (nextTerms[i] != null && nextTerms[i].getTerm().equals(termToProcess)) {
                // intermediate posting list for term 'termToProcess' in index 'i'

                String docsPath = PATH_TO_PARTIAL_INDEXES_DOCS + i;
                String freqsPath = PATH_TO_PARTIAL_INDEXES_FREQS + i;

                // retrieve posting list from partial inverted index file
                PostingList intermediatePostingList = new PostingList(vocabularyEntry, docsPath, freqsPath);

                // compute memory occupancy and add it to total space occupancy
                numBytes += intermediatePostingList.getNumBytes();

                // Append the posting list to the final posting list of the term
                finalList.appendPostings(intermediatePostingList.getPostings());

                //update vocabulary statistics
                vocabularyEntry.updateStatistics(intermediatePostingList);

                // Update the nextList array with the next term to process
                moveVocabulariesToNextTerm(termToProcess);
            }
        }

        // writing to vocabulary the space occupancy and memory offset of the posting list into
        vocabularyEntry.setMemorySize(numBytes);
        vocabularyEntry.setMemoryOffset(docsMemOffset);
        vocabularyEntry.setFrequencyOffset(freqsMemOffset);

        //compute the final idf
        vocabularyEntry.computeIDF();

        return finalList;
    }

    /**
     * Method to read the next term in these vocabularies in which we had the last processed term
     * @param processedTerm: last processed term, it is used to find which vocabularies must be read
     */
    private static void moveVocabulariesToNextTerm(String processedTerm) {

        // for each intermediate vocabulary
        for(int i=0; i<numIndexes; i++){
            // check if the last processed term was present in the i-th vocabulary
            if(nextTerms[i].getTerm().equals(processedTerm)) {
                // last processed term was present

                // update next memory offset to be read from the i-th vocabulary
                vocEntryMemOffset[i] += VOCETRY_SIZE;

                // read next vocabulary entry from the i-th vocabulary
                boolean ret = nextTerms[i].readFromDisk(vocEntryMemOffset[i], PATH_TO_PARTIAL_VOCABULARIES+i);

                // check if errors occurred while reading the vocabulary entry
                if(!ret){
                    nextTerms[i] = null;
                }
            }
        }
    }

    /**
     * Save to disk the posting list
     * @param list: the posting list to save
     * @return number of bytes written
     */
    private static long saveToDisk(PostingList list) {
        // memory occupancy of the posting list:
        // - for each posting we have to store 2 integers (docid and freq)
        // - each integer will occupy 4 bytes since we are storing integers in byte arrays
        int numBytes = list.getNumBytes();

        // try to open a file channel to the file of the inverted index
        try (FileChannel docsFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_INVERTED_INDEX_DOCS), StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE);
             FileChannel freqsFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_INVERTED_INDEX_FREQS), StandardOpenOption.WRITE,
                     StandardOpenOption.READ, StandardOpenOption.CREATE)
        ) {

            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, docsMemOffset, numBytes/2);

            // instantiation of MappedByteBuffer for integer list of freqs
            MappedByteBuffer freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, freqsMemOffset, numBytes/2);

            // check if MappedByteBuffers are correctly instantiated
            if (docsBuffer != null && freqsBuffer != null) {
                ArrayList<Map.Entry<Integer, Integer>> postings = list.getPostings();

                // write postings to file
                for (Map.Entry<Integer, Integer> posting : postings) {
                    // encode docid
                    docsBuffer.putInt(posting.getKey());
                    // encode freq
                    freqsBuffer.putInt(posting.getValue());
                }
                // update buffer positions
                docsMemOffset += docsBuffer.position();
                freqsMemOffset += freqsBuffer.position();

                return numBytes;
            }
        } catch (InvalidPathException e) {
            System.out.println("Path Error " + e);
        } catch (IOException e) {
            System.out.println("I/O Error " + e);
        }
        return -1;
    }

    /**
     * The effective merging pipeline:
     * - finds the minimum term between the indexes
     * - creates the whole posting list and the vocabulary entry for that term
     * - stores them in memory
     *
     * @return true if the merging is complete, false otherwise
     */
    public static boolean mergeIndexes(int numIndexes) {
        Merger.numIndexes = numIndexes;

        // initialization operations
        initialize();

        // next memory offset where to write the next vocabulary entry
        long vocMemOffset = 0;

        // open all the indexes in parallel and start merging their posting lists
        while(true){

            // find next term to be processed (the minimum in lexicographical order)
            String termToProcess = getMinTerm();

            if (termToProcess == null)
                break;

            // new vocabulary entry for the processed term
            VocabularyEntry vocabularyEntry = new VocabularyEntry(termToProcess);

            // merge the posting lists for the term to be processed
            PostingList mergedPostingList = processTerm(termToProcess, vocabularyEntry);

            // save posting list on disk
            saveToDisk(mergedPostingList);

            // save vocabulary entry on disk
            vocMemOffset += vocabularyEntry.write_entry_to_disk(vocMemOffset, PATH_TO_VOCABULARY);
            // TODO: refactor the name of this function to CamelCase, remove side effects (param position is changed)
        }

        cleanUp();
        return true;
    }

    /**
     * method to clean up the files:
     * - remove partial indexes
     * - remove partial vocabularies
     */
    private static void cleanUp() {
        // TODO: eliminate partial indexes and partial vocabularies
        for(int i=0; i<numIndexes; i++){
            // remove i-th partial vocabulary
            FileUtils.removeFile();

            // remove i-th partial index's docs file
            FileUtils.removeFile();

            // remove i-th partial index's freqs file
            FileUtils.removeFile();

            // remove directories ?
        }
    }
}
