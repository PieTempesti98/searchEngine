package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.beans.BlockDescriptor;
import it.unipi.dii.aide.mircv.common.beans.Posting;
import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.common.compression.UnaryCompressor;
import it.unipi.dii.aide.mircv.common.compression.VariableByteCompressor;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * Class that implements the merge of the intermediate posting lists during the SPIMI-Indexing algorithm
 */
public class Merger {

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
    private static String PATH_TO_PARTIAL_INDEXES_DOCS = ConfigurationParameters.getDocidsDir() + ConfigurationParameters.getDocidsFileName();

    /**
     * Standard pathname for partial index frequencies files
     */
    private static String PATH_TO_PARTIAL_INDEXES_FREQS = ConfigurationParameters.getFrequencyDir() + ConfigurationParameters.getFrequencyFileName();

    /**
     * Standard pathname for partial vocabulary files
     */
    private static String PATH_TO_PARTIAL_VOCABULARIES = ConfigurationParameters.getPartialVocabularyDir() + ConfigurationParameters.getVocabularyFileName();

    /**
     * Path to the inverted index docs file
     */
    private static String PATH_TO_INVERTED_INDEX_DOCS = ConfigurationParameters.getInvertedIndexDocs();

    /**
     * Path to the inverted index freqs file
     */
    private static String PATH_TO_INVERTED_INDEX_FREQS = ConfigurationParameters.getInvertedIndexFreqs();

    /**
     * Path to vocabulary
     */
    private static String PATH_TO_VOCABULARY = ConfigurationParameters.getVocabularyPath();

    /**
     * path to block descriptors file
     */
    private static String PATH_TO_BLOCK_DESCRIPTORS = ConfigurationParameters.getBlockDescriptorsPath();

    /**
     * Array used to point to the next vocabulary entry to process for each partial index
     */
    private static VocabularyEntry[] nextTerms = null;

    /**
     * memory offsets of last read vocabulary entry
     */
    private static long[] vocEntryMemOffset = null;

    private static FileChannel[] docidChannels = null;

    private static FileChannel[] frequencyChannels = null;
    /**
     * Method that initializes all the data structures:
     * - setting openIndexes to the total number of indexes produced by SPIMI
     * - initializing all the intermediate inverted index structures
     */
    private static boolean initialize() {

        // initialization of array of next vocabulary entries tp be processed
        nextTerms = new VocabularyEntry[numIndexes];

        // initialization of next memory offset to be read for each partial vocabulary
        vocEntryMemOffset = new long[numIndexes];

        // initialize the array of the file channels
        docidChannels = new FileChannel[numIndexes];
        frequencyChannels = new FileChannel[numIndexes];

        System.out.println("num indexes: "+numIndexes);

        try {
            for (int i = 0; i < numIndexes; i++) {
                nextTerms[i] = new VocabularyEntry();
                vocEntryMemOffset[i] = 0;

                // read first entry of the vocabulary
                long ret = nextTerms[i].readFromDisk(vocEntryMemOffset[i], PATH_TO_PARTIAL_VOCABULARIES + "_" + i);
                System.out.println("partial vocab: "+PATH_TO_PARTIAL_VOCABULARIES + "_" + i);
                System.out.println("read: "+nextTerms[i]);

                if (ret == -1 || ret == 0) {
                    // error encountered during vocabulary entry reading operation
                    // or read ended
                    nextTerms[i] = null;
                }
                docidChannels[i] = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_INDEXES_DOCS+ "_" + i),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );

                frequencyChannels[i] = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_INDEXES_FREQS+ "_" + i),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
            }
            return true;
        }catch(Exception e){
            cleanUp();
            e.printStackTrace();
            return false;
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
     * - update term statistics in the vocabulary entry (side effect)
     *
     * @param termToProcess: term to be processed
     * @param vocabularyEntry: vocabulary entry for new term
     * @return posting list of the processed term
     */
    private static PostingList processTerm(String termToProcess, VocabularyEntry vocabularyEntry) {
        // new posting list for the term
        PostingList finalList = new PostingList();
        finalList.setTerm(termToProcess);

        // processing the term
        for (int i = 0; i < numIndexes; i++) {

            // Found the matching term
            if (nextTerms[i] != null && nextTerms[i].getTerm().equals(termToProcess)) {

                // retrieve posting list from partial inverted index file
                PostingList intermediatePostingList = loadList(nextTerms[i], i);
                if(intermediatePostingList == null)
                    return null;

                // update max docLen
                vocabularyEntry.updateBM25Statistics(nextTerms[i].getBM25Tf(), nextTerms[i].getBM25Dl());

                //update vocabulary statistics
                vocabularyEntry.updateStatistics(intermediatePostingList);

                // Append the posting list to the final posting list of the term
                finalList.appendPostings(intermediatePostingList.getPostings());


            }
        }

        // Update the nextList array with the next term to process
        moveVocabulariesToNextTerm(termToProcess);

        // writing to vocabulary the space occupancy and memory offset of the posting list into
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
                vocEntryMemOffset[i] += VocabularyEntry.ENTRY_SIZE;

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
     * @param compressionMode flag deciding whether to compress posting lists or not
     * @param numIndexes number of partial vocabularies and partial indexes created
     * @return true if the merging is complete, false otherwise
     */
    public static boolean mergeIndexes(int numIndexes, boolean compressionMode, boolean debugMode) {

        Merger.numIndexes = numIndexes;

        // initialization operations
        if(!initialize())
            return false;

        //size of the vocabulary
        long vocSize = 0;

        // next memory offset where to write the next vocabulary entry
        long vocMemOffset = 0;

        System.out.println(PATH_TO_INVERTED_INDEX_DOCS);
        System.out.println(PATH_TO_INVERTED_INDEX_FREQS);

        // open file channels for vocabulary writes, docid and frequency writes, and block descriptor writes
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
                System.out.println("merged pl: "+mergedPostingList);
                if(mergedPostingList == null){
                    throw new Exception("ERROR: the merged posting list for the term " + termToProcess + " is null");
                }

                // compute information about block descriptors for the posting list to be written
                //vocabularyEntry.computeBlocksInformation(mergedPostingList.getPostings().size());
                vocabularyEntry.computeBlocksInformation();

                // compute maximal number of postings that can be stored in a block
                int maxNumPostings = vocabularyEntry.getMaxNumberOfPostingsInBlock();

                // create iterator over posting list to be written
                Iterator<Posting> plIterator = mergedPostingList.getPostings().iterator();

                int numBlocks = vocabularyEntry.getNumBlocks();

                // save posting list on disk writing each block
                for(int i=0; i< numBlocks; i++){

                    // create a new block descriptor and update its information
                    BlockDescriptor blockDescriptor = new BlockDescriptor();
                    blockDescriptor.setDocidOffset(docsMemOffset);
                    blockDescriptor.setFreqOffset(freqsMemOffset);

                    // number of postings written in the block
                    int postingsInBlock = 0;

                    int alreadyWrittenPostings = i*maxNumPostings;

                    // number of postings to be written in the current block
                    int nPostingsToBeWritten = (Math.min((mergedPostingList.getPostings().size() - alreadyWrittenPostings), maxNumPostings));

                    if(compressionMode){
                        // arrays where to store docids and frequencies to be written in current block
                        int[] docids = new int[nPostingsToBeWritten];
                        int[] freqs = new int[nPostingsToBeWritten];

                        // initialize docids and freqs arrays
                        while(true){
                            // get next posting to be written to disk
                            Posting currPosting = plIterator.next();
                            docids[postingsInBlock] = currPosting.getDocid();
                            freqs[postingsInBlock] = currPosting.getFrequency();

                            postingsInBlock++;

                            if (postingsInBlock == nPostingsToBeWritten){

                                byte[] compressedDocs = VariableByteCompressor.integerArrayCompression(docids);
                                byte[] compressedFreqs = UnaryCompressor.integerArrayCompression(freqs);

                                System.out.println("compressed docids in merger:");
                                for(int a = 0; a<compressedDocs.length; a++){
                                    System.out.println(compressedDocs[a]);
                                }

                                try{
                                    // instantiation of MappedByteBuffer for integer list of docids and for integer list of freqs
                                    MappedByteBuffer docsBuffer = docidChan.map(FileChannel.MapMode.READ_WRITE, docsMemOffset, compressedDocs.length);
                                    MappedByteBuffer freqsBuffer = frequencyChan.map(FileChannel.MapMode.READ_WRITE, freqsMemOffset, compressedFreqs.length);

                                    // write compressed posting lists to disk
                                    docsBuffer.put(compressedDocs);
                                    freqsBuffer.put(compressedFreqs);

                                    System.out.println("decompressed docids in merger:");
                                    for(int a = 0; a<docids.length; a++){
                                        System.out.println(docids[a]);
                                    }

                                    // update the size of the block
                                    blockDescriptor.setDocidSize(compressedDocs.length);
                                    blockDescriptor.setFreqSize(compressedFreqs.length);

                                    // update the max docid of the block
                                    blockDescriptor.setMaxDocid(currPosting.getDocid());

                                    // update the number of postings in the block
                                    blockDescriptor.setNumPostings(postingsInBlock);

                                    // write the block descriptor on disk
                                    blockDescriptor.saveDescriptorOnDisk(descriptorChan);

                                    docsMemOffset+=compressedDocs.length;
                                    freqsMemOffset+=compressedFreqs.length;
                                    break;

                                } catch (Exception e) {
                                    cleanUp();
                                    e.printStackTrace();
                                    return false;
                                }
                            }
                        }
                    } else {
                        // posting list must not be compressed

                        // set docs and freqs num bytes as (number of postings)*4
                        blockDescriptor.setDocidSize(nPostingsToBeWritten*4);
                        blockDescriptor.setFreqSize(nPostingsToBeWritten*4);

                        // write postings to block
                        try {
                            // instantiation of MappedByteBuffer for integer list of docids and for integer list of freqs
                            MappedByteBuffer docsBuffer = docidChan.map(FileChannel.MapMode.READ_WRITE, docsMemOffset, nPostingsToBeWritten* 4L);
                            MappedByteBuffer freqsBuffer = frequencyChan.map(FileChannel.MapMode.READ_WRITE, freqsMemOffset, nPostingsToBeWritten* 4L);

                            if (docsBuffer != null && freqsBuffer != null) {
                                while (true) {
                                    // get next posting to be written to disk
                                    Posting currPosting = plIterator.next();

                                    // encode docid and freq
                                    docsBuffer.putInt(currPosting.getDocid());
                                    freqsBuffer.putInt(currPosting.getFrequency());

                                    // increment counter of number of postings written in the block
                                    postingsInBlock++;

                                    // check if currPosting is the last posting to be written in the current block
                                    if (postingsInBlock == nPostingsToBeWritten) {
                                        // update the max docid of the block
                                        blockDescriptor.setMaxDocid(currPosting.getDocid());

                                        // update the number of postings in the block
                                        blockDescriptor.setNumPostings(postingsInBlock);

                                        // write the block descriptor on disk
                                        blockDescriptor.saveDescriptorOnDisk(descriptorChan);

                                        docsMemOffset+=nPostingsToBeWritten*4L;
                                        freqsMemOffset+=nPostingsToBeWritten*4L;
                                        break;
                                    }
                                }
                            }
                        }
                        catch (Exception e){
                            cleanUp();
                            e.printStackTrace();
                        }
                    }
                }
                // save vocabulary entry on disk
                vocMemOffset = vocabularyEntry.writeEntryToDisk(vocMemOffset, vocabularyChan);
                vocSize++;

                if(debugMode){
                    mergedPostingList.debugSaveToDisk("debugDOCIDS.txt", "debugFREQS.txt", maxNumPostings);
                    vocabularyEntry.debugSaveToDisk("debugVOCABULARY.txt");
                }
            }

            cleanUp();
            CollectionSize.updateVocabularySize(vocSize);
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
        try{
            for(int i = 0; i < numIndexes; i++){
                if(docidChannels[i] != null){
                    docidChannels[i].close();
                }
                if(frequencyChannels[i] != null){
                    frequencyChannels[i].close();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private static PostingList loadList(VocabularyEntry term, int index) {
        PostingList newList;

        try {
            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docBuffer = docidChannels[index].map(
                    FileChannel.MapMode.READ_ONLY,
                    term.getDocidOffset(),
                    term.getDocidSize()
            );

            // instantiation of MappedByteBuffer for integer list of frequencies
            MappedByteBuffer freqBuffer = frequencyChannels[index].map(
                    FileChannel.MapMode.READ_ONLY,
                    term.getFrequencyOffset(),
                    term.getFrequencySize()
            );

            // create the posting list for the term
            newList = new PostingList(term.getTerm());

            for (int i = 0; i < term.getDf(); i++) {
                Posting posting = new Posting(docBuffer.getInt(), freqBuffer.getInt());
                newList.getPostings().add(posting);
            }
            return newList;
        } catch (Exception e) {
            cleanUp();
            e.printStackTrace();
            return null;
        }
    }

    /**
     * needed for testing purposes
     * @param pathToVocabulary: path to be set as vocabulary path
     */
    public static void setPathToVocabulary(String pathToVocabulary) {
        PATH_TO_VOCABULARY = pathToVocabulary;
    }

    /**
     * needed for testing purposes
     * @param pathToInvertedIndexDocs: path to be set as inverted index's docs path
     */
    public static void setPathToInvertedIndexDocs(String pathToInvertedIndexDocs) {
        PATH_TO_INVERTED_INDEX_DOCS = pathToInvertedIndexDocs;
    }
    
    /**
     * needed for testing purposes
     * @param invertedIndexFreqs: path to be set as inverted index's freqs path
     */
    public static void setPathToInvertedIndexFreqs(String invertedIndexFreqs) { PATH_TO_INVERTED_INDEX_FREQS = invertedIndexFreqs;}

    /**
     * needed for testing purposes
     * @param blockDescriptorsPath: path to be set as block descriptors' path
     */
    public static void setPathToBlockDescriptors(String blockDescriptorsPath) { PATH_TO_BLOCK_DESCRIPTORS = blockDescriptorsPath;}

    /**
     * needed for testing purposes
     * @param pathToPartialIndexesDocs: path to be set
     */
    public static void setPathToPartialIndexesDocs(String pathToPartialIndexesDocs) { PATH_TO_PARTIAL_INDEXES_DOCS = pathToPartialIndexesDocs;}

    /**
     * needed for testing purposes
     * @param pathToPartialIndexesFreqs: path to be set
     */
    public static void setPathToPartialIndexesFreqs(String pathToPartialIndexesFreqs) { PATH_TO_PARTIAL_INDEXES_FREQS = pathToPartialIndexesFreqs;}

    /**
     * needed for testing purposes
     * @param pathToPartialVocabularies: path to be set
     */
    public static void setPathToPartialVocabularies(String pathToPartialVocabularies) { PATH_TO_PARTIAL_VOCABULARIES = pathToPartialVocabularies;}
}