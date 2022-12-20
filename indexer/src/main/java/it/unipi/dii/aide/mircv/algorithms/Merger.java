package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.beans.BlockDescriptor;
import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;


/**
 * Class that implements the merge of the intermediate posting lists during the SPIMI-Indexing algorithm
 */
public class Merger {

    private static final long VOCENTRY_SIZE = VocabularyEntry.ENTRY_SIZE;
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
    private static final String PATH_TO_PARTIAL_INDEXES_DOCS = ConfigurationParameters.getDocidsDir() + ConfigurationParameters.getDocidsFileName();

    /**
     * Standard pathname for partial index frequencies files
     */
    private static final String PATH_TO_PARTIAL_INDEXES_FREQS = ConfigurationParameters.getFrequencyDir() + ConfigurationParameters.getFrequencyFileName();

    /**
     * Standard pathname for partial vocabulary files
     */
    private static final String PATH_TO_PARTIAL_VOCABULARIES = ConfigurationParameters.getPartialVocabularyDir() + ConfigurationParameters.getVocabularyFileName();

    /**
     * Path to the inverted index docs file
     */
    private final static String PATH_TO_INVERTED_INDEX_DOCS = ConfigurationParameters.getInvertedIndexDocs();

    /**
     * Path to the inverted index freqs file
     */
    private final static String PATH_TO_INVERTED_INDEX_FREQS = ConfigurationParameters.getInvertedIndexFreqs();

    /**
     * Path to vocabulary
     */
    private static final String PATH_TO_VOCABULARY = ConfigurationParameters.getVocabularyPath();

    /**
     * path to block descriptors file
     */
    private static final String PATH_TO_BLOCK_DESCRIPTORS = ConfigurationParameters.getBlockDescriptorsPath();

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
            long ret = nextTerms[i].readFromDisk(vocEntryMemOffset[i], PATH_TO_PARTIAL_VOCABULARIES+"_"+i);

            if(ret == -1 || ret == 0){
                // error encountered during vocabulary entry reading operation
                // or read ended
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
        int numBytes = 0;

        // processing the term
        for (int i = 0; i < numIndexes; i++) {

            // Found the matching term
            if (nextTerms[i] != null && nextTerms[i].getTerm().equals(termToProcess)) {
                // intermediate posting list for term 'termToProcess' in index 'i'

                String docsPath = PATH_TO_PARTIAL_INDEXES_DOCS + "_" + i;
                String freqsPath = PATH_TO_PARTIAL_INDEXES_FREQS + "_" + i;

                // retrieve posting list from partial inverted index file
                PostingList intermediatePostingList = new PostingList(nextTerms[i], docsPath, freqsPath);

                // update max docLen
                vocabularyEntry.updateMaxDl(nextTerms[i].getMaxDl());

                // compute memory occupancy and add it to total space occupancy
                numBytes += intermediatePostingList.getNumBytes();

                //update vocabulary statistics
                vocabularyEntry.updateStatistics(intermediatePostingList);

                // Append the posting list to the final posting list of the term
                finalList.appendPostings(intermediatePostingList.getPostings());


            }
        }

        // Update the nextList array with the next term to process
        moveVocabulariesToNextTerm(termToProcess);

        // writing to vocabulary the space occupancy and memory offset of the posting list into
        vocabularyEntry.setDocidSize(numBytes);
        vocabularyEntry.setFrequencySize(numBytes);
        vocabularyEntry.setMemoryOffset(docsMemOffset);
        vocabularyEntry.setFrequencyOffset(freqsMemOffset);

        // compute the final idf
        vocabularyEntry.computeIDF();

        // compute the term upper bounds
        vocabularyEntry.computeUpperBounds();

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
            if(nextTerms[i] != null && nextTerms[i].getTerm().equals(processedTerm)) {
                // last processed term was present

                // update next memory offset to be read from the i-th vocabulary
                vocEntryMemOffset[i] += VOCENTRY_SIZE;

                // read next vocabulary entry from the i-th vocabulary
                long ret = nextTerms[i].readFromDisk(vocEntryMemOffset[i], PATH_TO_PARTIAL_VOCABULARIES+ "_" +i);

                // check if errors occurred while reading the vocabulary entry
                if(ret == -1 || ret == 0){
                    // read ended or an error occurred
                    nextTerms[i] = null;
                }
            }
        }
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
        try(FileChannel vocabularyChan = (FileChannel) Files.newByteChannel(
                Paths.get(PATH_TO_VOCABULARY),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
            FileChannel docidChan = (FileChannel) Files.newByteChannel(
                    Paths.get(PATH_TO_INVERTED_INDEX_DOCS),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);
            FileChannel frequencyChan = (FileChannel) Files.newByteChannel(
                    Paths.get(PATH_TO_INVERTED_INDEX_FREQS),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);
            FileChannel descriptorChan = (FileChannel) Files.newByteChannel(
                    Paths.get(PATH_TO_BLOCK_DESCRIPTORS),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE)
        ) {
            // open all the indexes in parallel and start merging their posting lists
            while (true) {

                // find next term to be processed (the minimum in lexicographical order)
                String termToProcess = getMinTerm();

                if (termToProcess == null)
                    break;

                // new vocabulary entry for the processed term
                VocabularyEntry vocabularyEntry = new VocabularyEntry(termToProcess);

                // merge the posting lists for the term to be processed
                PostingList mergedPostingList = processTerm(termToProcess, vocabularyEntry);

                // save posting list on disk and update offsets
                // instantiation of MappedByteBuffer for integer list of docids
                MappedByteBuffer docsBuffer = docidChan.map(FileChannel.MapMode.READ_WRITE, docsMemOffset, vocabularyEntry.getDocidSize());

                // instantiation of MappedByteBuffer for integer list of freqs
                MappedByteBuffer freqsBuffer = frequencyChan.map(FileChannel.MapMode.READ_WRITE, freqsMemOffset, vocabularyEntry.getFrequencySize());

                // check if MappedByteBuffers are correctly instantiated
                if (docsBuffer != null && freqsBuffer != null) {
                    // create and initialize the first block descriptor for the posting list
                    BlockDescriptor blockDescriptor = new BlockDescriptor();
                    blockDescriptor.setDocidOffset(docsMemOffset);
                    blockDescriptor.setFreqOffset(freqsMemOffset);
                    int maxNumPostings = (int)Math.ceil(vocabularyEntry.getDf() / (double)vocabularyEntry.getNumBlocks());
                    int currentBlock = 1;
                    int postingsInBlock = 0;
                    // write postings to file
                    for (Map.Entry<Integer, Integer> posting : mergedPostingList.getPostings()) {
                        // encode docid
                        docsBuffer.putInt(posting.getKey());
                        // encode freq
                        freqsBuffer.putInt(posting.getValue());
                        postingsInBlock ++;
                        if(postingsInBlock == maxNumPostings && currentBlock < vocabularyEntry.getNumBlocks()){
                            // update the size of the block
                            blockDescriptor.setDocidSize((int) (docsMemOffset + docsBuffer.position() - blockDescriptor.getDocidOffset()));
                            blockDescriptor.setFreqSize((int) (freqsMemOffset + freqsBuffer.position() - blockDescriptor.getFreqOffset()));

                            // update the max docid of the block
                            blockDescriptor.setMaxDocid(posting.getKey());

                            // update the number of postings in the block
                            blockDescriptor.setNumPostings(postingsInBlock);

                            // write the block descriptor on disk
                            blockDescriptor.saveDescriptorOnDisk(descriptorChan);

                            // create the new block descriptor
                            blockDescriptor = new BlockDescriptor();
                            currentBlock ++;
                            blockDescriptor.setDocidOffset(docsMemOffset + docsBuffer.position());
                            blockDescriptor.setFreqOffset(freqsMemOffset + freqsBuffer.position());
                        }
                    }

                /* DEBUG
                System.out.println("new offsets:\tdocs:"+(docsMemOffset + docsBuffer.position())+" freqs:"+(freqsMemOffset + freqsBuffer.position()));
                */

                    docsMemOffset += docsBuffer.position();
                    freqsMemOffset += freqsBuffer.position();
                    // save vocabulary entry on disk
                    vocMemOffset = vocabularyEntry.writeEntryToDisk(vocMemOffset, vocabularyChan);
                }
            }

            cleanUp();
            return true;
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * method to clean up the files:
     * - remove partial indexes
     * - remove partial vocabularies
     */
    private static void cleanUp() {
        // remove partial index docids directory
        FileUtils.deleteDirectory(ConfigurationParameters.getDocidsDir());

        // remove partial index frequencies directory
        FileUtils.deleteDirectory(ConfigurationParameters.getFrequencyDir());

        // remove partial vocabularies directory
        FileUtils.deleteDirectory(ConfigurationParameters.getPartialVocabularyDir());
    }
}
