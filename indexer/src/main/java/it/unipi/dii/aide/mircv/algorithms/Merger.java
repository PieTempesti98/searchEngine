package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

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

    /**
     * Inverted index's next free memory offset
     */
    private static long memOffset = 0;

    /**
     * Standard pathname for the intermediate index files
     */
    private static final String PARTIAL_INDEX_PATH = ConfigurationParameters.getPartialIndexPath();

    /**
     * number of intermediate indexes produced by SPIMI algorithm
     */
    private static int numIndexes;

    /**
     * vocabulary path to mapped db
     */
    private static final String PATH_TO_VOCABULARY = ConfigurationParameters.getVocabularyPath();

    /**
     * memory mapped intermediate indexes
     */
    private static final Map<Integer, Iterator<PostingList>> intermediateIndexes = new HashMap<>();

    /**
     * vocabulary for the final inverted index
     */
    private static Map<String, VocabularyEntry> vocabulary;

    private final static String PATH_TO_INVERTED_INDEX = ConfigurationParameters.getInvertedIndexPath();

    private static PostingList[] nextLists = null;

    /**
     * Method that initializes all the data structures:
     * - setting openIndexes to the total number of indexes produced by SPIMI
     * - initializing all the intermediate inverted index structures
     */
    private static void initialize(DB dbInd, DB dbVoc) {

        // open the vocabulary
        vocabulary = (Map<String, VocabularyEntry>) dbVoc.hashMap("vocabulary")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();

        nextLists = new PostingList[numIndexes];
        // get all the intermediate indexes
        for (int i = 0; i < numIndexes; i++) {
            List<PostingList> list = (List<PostingList>) dbInd.indexTreeList("index_" + i, Serializer.JAVA).createOrOpen();
            intermediateIndexes.put(i, list.iterator());
            nextLists[i] = intermediateIndexes.get(i).next();
        }
    }

    /**
     * Return the minimum term of the terms to be processed in the intermediate indexes
     *
     * @return the next term to process
     */
    private static String getMinTerm() {
        String term = null;

        for (int i = 0; i < numIndexes; i++) {

            // check if there are still posting lists to be processed at intermediate index 'i'
            if(nextLists[i] == null)
                continue;


            // next term to be processed at the intermediate index 'i'
            String nextTerm = nextLists[i].getTerm();

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
     * @param term: term to be processed
     * @return posting list of the processed term
     */
    private static PostingList processTerm(String term, VocabularyEntry vocabularyEntry) {
        // new posting list for the term
        PostingList finalList = new PostingList();
        finalList.setTerm(term);

        // total space occupancy in bytes of the final posting list
        long numBytes = 0;

        // processing the term
        for (int i = 0; i < numIndexes; i++) {
            // Found the matching term
            if (nextLists[i] != null) {
                // intermediate posting list for term 'termToProcess' in index 'i'
                PostingList intermediatePostingList = nextLists[i];
                if (intermediatePostingList.getTerm().equals(term)) {

                    // compute memory occupancy and add it to total space occupancy
                    numBytes += intermediatePostingList.getNumBytes();

                    // Append the posting list to the final posting list of the term
                    finalList.appendPostings(intermediatePostingList.getPostings());

                    //update vocabulary statistics
                    vocabularyEntry.updateStatistics(intermediatePostingList);

                    // Update the nextList array with the next term to process
                    if(intermediateIndexes.get(i).hasNext())
                        nextLists[i] = intermediateIndexes.get(i).next();
                    else{
                        // set index 'i' to null since it is empty
                        nextLists[i] = null;
                    }
                }
            }
        }

        // writing to vocabulary the space occupancy and memory offset of the posting list into
        vocabularyEntry.setMemorySize(numBytes);
        vocabularyEntry.setMemoryOffset(memOffset);

        //compute the final idf
        vocabularyEntry.computeIDF();

        return finalList;
    }


    private static long saveToDisk(PostingList list, VocabularyEntry vocEntry) {
        // memory occupancy of the posting list:
        // - for each posting we have to store 2 integers (docid and freq)
        // - each integer will occupy 4 bytes since we are storing integers in byte arrays
        int numBytes = list.getNumBytes();

        // try to open a file channel to the file of the inverted index
        try (FileChannel fChan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_INVERTED_INDEX), StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE)){

            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memOffset, numBytes);

            // check if MappedByteBuffers are correctly instantiated
            if (buffer != null) {
                ArrayList<Map.Entry<Integer, Integer>> postings = list.getPostings();

                // write postings to file
                for (Map.Entry<Integer, Integer> posting : postings) {
                    // encode docid
                    buffer.putInt(posting.getKey());
                }

                // set the frequency offset in the vocabulary
                vocEntry.setFrequencyOffset(memOffset + buffer.position());

                for (Map.Entry<Integer, Integer> posting : postings) {
                    // encode frequency
                    buffer.putInt(posting.getValue());
                }
                long memorySize = buffer.position();
                memOffset += buffer.position();
                vocEntry.setMemorySize(memorySize);
                vocabulary.put(vocEntry.getTerm(), vocEntry);
                return memorySize;
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

        try (DB dbVoc = DBMaker.fileDB(PATH_TO_VOCABULARY).fileChannelEnable().fileMmapEnable().make(); // vocabulary memory mapped file
             DB dbInd = DBMaker.fileDB(PARTIAL_INDEX_PATH).fileChannelEnable().fileMmapEnable().make() // intermediate indexes memory mapped file
        ) {

            // initialization operations
            initialize(dbInd, dbVoc);

            // open all the indexes in parallel and start merging their posting lists
            while (true) {

                // find next term to be processed (the minimum in lexicographical order)
                String termToProcess = getMinTerm();
                // System.out.println(termToProcess);

                if(termToProcess == null)
                    break;
                VocabularyEntry vocabularyEntry = new VocabularyEntry(termToProcess);
                // merge the posting lists for the term to be processed
                PostingList mergedPostingList = processTerm(termToProcess, vocabularyEntry);

                // save it on disk and compute the information to store in vocabulary
                long memorySize = saveToDisk(mergedPostingList, vocabularyEntry);

            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static  List<PostingList> testMerger(Map<Integer, List<PostingList>> partialIndexes){

        List<PostingList> finalIndex = new ArrayList<>();

        numIndexes = partialIndexes.size();

        // initialization
        try (DB dbVoc = DBMaker.fileDB(PATH_TO_VOCABULARY).fileChannelEnable().fileMmapEnable().make(); // vocabulary memory mapped file
        ){
            // open the vocabulary
            vocabulary = (Map<String, VocabularyEntry>) dbVoc.hashMap("vocabulary")
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(Serializer.JAVA)
                    .createOrOpen();
            nextLists = new PostingList[numIndexes];

            // get all the intermediate indexes
            for (int i = 0; i < numIndexes; i++) {
                intermediateIndexes.put(i, partialIndexes.get(i).iterator());
                nextLists[i] = intermediateIndexes.get(i).next();
            }

            while(true){
                String termToProcess = getMinTerm();

                if(termToProcess == null)
                    break;

                // merge the posting lists for the term to be processed
                PostingList mergedPostingList = processTerm(termToProcess);

                finalIndex.add(mergedPostingList);
            }

            return finalIndex;
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static  List<VocabularyEntry> testVocabularyCreation(Map<Integer, List<PostingList>> partialIndexes){

        List<PostingList> finalIndex = new ArrayList<>();

        numIndexes = partialIndexes.size();

        List<VocabularyEntry> testVocabulary = new ArrayList<>();

        // initialization
        try (DB dbVoc = DBMaker.fileDB(PATH_TO_VOCABULARY).fileChannelEnable().fileMmapEnable().make(); // vocabulary memory mapped file
        ){
            // open the vocabulary
            vocabulary = (Map<String, VocabularyEntry>) dbVoc.hashMap("vocabulary")
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(Serializer.JAVA)
                    .createOrOpen();
            nextLists = new PostingList[numIndexes];

            // get all the intermediate indexes
            for (int i = 0; i < numIndexes; i++) {
                intermediateIndexes.put(i, partialIndexes.get(i).iterator());
                nextLists[i] = intermediateIndexes.get(i).next();
            }

            while(true){
                String termToProcess = getMinTerm();

                if(termToProcess == null)
                    break;

                // merge the posting lists for the term to be processed
                PostingList mergedPostingList = processTerm(termToProcess);

                finalIndex.add(mergedPostingList);

                VocabularyEntry testVocEntry = vocabulary.get(termToProcess);
                testVocabulary.add(testVocEntry);
            }

            return testVocabulary;
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
