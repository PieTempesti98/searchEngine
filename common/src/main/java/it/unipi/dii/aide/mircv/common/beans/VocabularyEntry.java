package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Objects;


/**
 * Entry of the vocabulary for a term
 */
public class VocabularyEntry {

    private static String BLOCK_DESCRIPTORS_PATH = ConfigurationParameters.getBlockDescriptorsPath();

    /* --- TERM INFORMATION --- */
    /**
     * incremental counter of the terms, used to assign the termid
     */
    private static int termCount = 0;

    /**
     * termid of the specific term
     */
    private int termid;

    /**
     * Term to which refers the vocabulary entry
     */
    private String term;

    /* --- TFIDF PARAMETERS --- */
    /**
     * Document frequency of the term
     */
    private int df = 0;

    /**
     * inverse of document frequency of the term
     */
    private double idf = 0;

    /* --- INFORMATION NEEDED TO IMPLEMENT MAX_SCORE --- */
    /**
     * maximum term frequency of the term
     */
    private int maxTf = 0;

    /**
     * maximum document length across the documents in which the term is present
     */
    private int BM25Dl = 1;

    private int BM25Tf = 0;

    /**
     * maximum value of TFIDF for the term
     */
    private double maxTFIDF = 0;

    /**
     * maximum value of BM25 for the term
     */
    private double maxBM25 = 0;

    public int getBM25Dl() {
        return BM25Dl;
    }

    public void setBM25Dl(int BM25Dl) {
        this.BM25Dl = BM25Dl;
    }

    public int getBM25Tf() {
        return BM25Tf;
    }

    public void setBM25Tf(int BM25Tf) {
        this.BM25Tf = BM25Tf;
    }

    /** needed for testing purposes **/
    public static void setBlockDescriptorsPath(String blockDescriptorsPath) {
        BLOCK_DESCRIPTORS_PATH = blockDescriptorsPath;
    }


    /**
     * method to update the max document length for the term
     *
     * @param dl the new document length to process
     */
    public void updateBM25Statistics(int tf, int dl) {
        double currentRatio = (double) this.BM25Tf / (double) (this.BM25Dl + this.BM25Tf);
        double newRatio = (double) tf / (double) (dl + tf);
        if(newRatio > currentRatio){
            this.BM25Tf = tf;
            this.BM25Dl = dl;
        }
    }

    /* --- MEMORY INFORMATION --- */
    /**
     * starting point of the term's posting list in the inverted index in bytes
     */
    private long docidOffset = 0;

    /**
     * Starting point of the frequencies in the inverted index in bytes
     */
    private long frequencyOffset = 0;


    /**
     * size of the term's posting list in the docid file of the inverted index in bytes
     */
    private int docidSize = 0;

    /**
     * size of the term's posting list in the frequency file of the inverted index in bytes
     */
    private int frequencySize = 0;

    /**
     * number of blocks in which the posting list is divided; 1 is the default value (all the postings in the same block)
     */
    private int numBlocks = 1;

    /**
     * start offset of the block descriptors in the block descriptor file
     */
    private long blockOffset = 0;

    /**
     * size of the term; if a term is greater than this size it'll be truncated
     */
    public static final int TERM_SIZE = 64;

    /**
     * we have to store the term size plus 7 ints, 3 double and 3 longs, total 136 bytes
     */
    public static final long ENTRY_SIZE = TERM_SIZE + 76;

    /**
     * Constructor for the vocabulary entry
     * create an empty class
     */
    public VocabularyEntry() {
    }

    /**
     * Constructor for the vocabulary entry for the term passed as parameter
     * Assign the termid to the term and initializes all the statistics and memory information
     *
     * @param term the token of the entry
     */
    public VocabularyEntry(String term) {

        // Assign the term
        this.term = term;

        // Assign the termid and increase the counter
        this.termid = termCount;
        termCount++;
    }

    /**
     * s the statistics of the vocabulary:
     * updates tf and df with the data of the partial posting list processed
     *
     * @param list the posting list from which the method computes the statistics
     */
    public void updateStatistics(PostingList list) {

        //for each element of the intermediate posting list
        for (Posting posting : list.getPostings()) {

            // update the max term frequency
            if (posting.getFrequency() > this.maxTf)
                this.maxTf = posting.getFrequency();

            // update the raw document frequency
            this.df++;
        }
    }

    /**
     * Compute the idf using the values computed during the merging of the indexes
     */
    public void computeIDF() {
        this.idf = Math.log10(CollectionSize.getCollectionSize() / (double) this.df);
    }

    public void setDocidSize(int docidSize) {
        this.docidSize = docidSize;
    }

    public void setFrequencySize(int frequencySize) {
        this.frequencySize = frequencySize;
    }

    public void setMemoryOffset(long memoryOffset) {
        this.docidOffset = memoryOffset;
    }

    public void setFrequencyOffset(long freqOffset) {
        this.frequencyOffset = freqOffset;
    }

    public void setDf(int df) {
        this.df = df;
    }

    /**
     * @param fChan    : fileChannel of the vocabulary file
     * @param position : position to start writing from
     * @return offset representing the position of the last written byte
     */
    public long writeEntryToDisk(long position, FileChannel fChan) {
        // instantiation of MappedByteBuffer
        try {
            MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE, position, ENTRY_SIZE);

            // Buffer not created
            if (buffer == null)
                return -1;

            //allocate char buffer to write term
            CharBuffer charBuffer = CharBuffer.allocate(TERM_SIZE);

            //populate char buffer char by char
            for (int i = 0; i < term.length(); i++)
                charBuffer.put(i, term.charAt(i));

            // Write the term into file
            buffer.put(StandardCharsets.UTF_8.encode(charBuffer));

            // write statistics
            buffer.putInt(df);
            buffer.putDouble(idf);

            // write term upper bound information
            buffer.putInt(maxTf);
            buffer.putInt(BM25Dl);
            buffer.putInt(BM25Tf);
            buffer.putDouble(maxTFIDF);
            buffer.putDouble(maxBM25);

            // write memory information
            buffer.putLong(docidOffset);
            buffer.putLong(frequencyOffset);
            buffer.putInt(docidSize);
            buffer.putInt(frequencySize);

            // write block information
            buffer.putInt(numBlocks);
            buffer.putLong(blockOffset);

            // return position for which we have to start writing on file
            return position + ENTRY_SIZE;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * Read the document index entry from disk
     *
     * @param memoryOffset the memory offset from which we start reading
     * @param PATH         path of the file on disk
     * @return the position of the last byte read
     */
    public long readFromDisk(long memoryOffset, String PATH) {
        // try to open a file channel to the file of the inverted index
        try (FileChannel fChan = (FileChannel) Files.newByteChannel(
                Paths.get(PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE)) {

            // instantiation of MappedByteBuffer for the PID read
            MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_ONLY, memoryOffset, ENTRY_SIZE);

            // Buffer not created
            if (buffer == null)
                return -1;

            // Read from file into the charBuffer, then pass to the string
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

            String[] encodedTerm = charBuffer.toString().split("\0");
            if (encodedTerm.length == 0) // TODO: no more entries to read
                return 0;

            this.term = encodedTerm[0];

            // Instantiate the buffer for reading other information
            buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memoryOffset + TERM_SIZE, ENTRY_SIZE - TERM_SIZE);

            // Buffer not created
            if (buffer == null)
                return -1;

            // read statistics
            df = buffer.getInt();
            idf = buffer.getDouble();

            // read term upper bound information
            maxTf = buffer.getInt();
            BM25Dl = buffer.getInt();
            BM25Tf = buffer.getInt();
            maxTFIDF = buffer.getDouble();
            maxBM25 = buffer.getDouble();

            // read memory information
            docidOffset = buffer.getLong();
            frequencyOffset = buffer.getLong();
            docidSize = buffer.getInt();
            frequencySize = buffer.getInt();

            // write block information
            numBlocks = buffer.getInt();
            blockOffset = buffer.getLong();

            return memoryOffset + ENTRY_SIZE;

        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * method used to compute the max TFIDF and BM25 used as term upper bounds
     */
    public void computeUpperBounds(){

        // compute term upper bound for TFIDF
        this.maxTFIDF = (1 + Math.log10(this.maxTf)) * this.idf;

        double k1 = 1.5;
        double b = 0.75;
        double avgDocLen = (double) CollectionSize.getTotalDocLen()/CollectionSize.getCollectionSize();

        this.maxBM25 = (idf * BM25Tf)  / ( BM25Tf + k1 * (1 - b + b * (double)BM25Dl/avgDocLen));

    }

    /**
     * method that computes the number of blocks of postings in which the posting list will be divided.
     * If the number of postings is < 1024 the posting list is stored in a single block.
     */
    public void computeBlocksInformation(){
        this.blockOffset = BlockDescriptor.getMemoryOffset();
        if(df >= 1024)
            this.numBlocks = (int)Math.ceil(Math.sqrt(df));
    }

    public int getMaxNumberOfPostingsInBlock(){
        return (int) Math.ceil( df / (double) numBlocks);
    }

    public String getTerm() {
        return term;
    }

    public int getDf() {
        return df;
    }

    public int getMaxTf() {
        return maxTf;
    }

    public double getIdf() {
        return idf;
    }

    public long getDocidOffset() {
        return docidOffset;
    }

    public long getFrequencyOffset() {
        return frequencyOffset;
    }

    public int getDocidSize() {
        return docidSize;
    }

    public int getFrequencySize() {
        return frequencySize;
    }

    public static int getTermCount() {
        return termCount;
    }

    public static void setTermCount(int termCount) {
        VocabularyEntry.termCount = termCount;
    }

    public int getTermid() {
        return termid;
    }

    public void setTermid(int termid) {
        this.termid = termid;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setIdf(double idf) {
        this.idf = idf;
    }

    public void setMaxTf(int maxTf) {
        this.maxTf = maxTf;
    }

    public double getMaxTFIDF() {
        return maxTFIDF;
    }

    public void setMaxTFIDF(double maxTFIDF) {
        this.maxTFIDF = maxTFIDF;
    }

    public double getMaxBM25() {
        return maxBM25;
    }

    public void setMaxBM25(double maxBM25) {
        this.maxBM25 = maxBM25;
    }

    public int getNumBlocks() {
        return numBlocks;
    }

    public void setNumBlocks(int numBlocks) {
        this.numBlocks = numBlocks;
    }

    public long getBlockOffset() {
        return blockOffset;
    }

    public void setBlockOffset(long blockOffset) {
        this.blockOffset = blockOffset;
    }

    /**

     * function to write a summarization of the most important data about a vocabulary entry as plain text in the debug file
     * @param path: path of the file where to write
     */
    public void debugSaveToDisk(String path) {
        FileUtils.createDirectory("data/debug");
        FileUtils.createIfNotExists("data/debug/"+path);

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("data/debug/"+path, true));
            writer.write(this+"\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * method to read from memory the block descriptors for the term
     * @return the arrayList of the block descriptors
     */
    public ArrayList<BlockDescriptor> readBlocks(){
        try(
                FileChannel fileChannel = (FileChannel) Files.newByteChannel(
                        Paths.get(BLOCK_DESCRIPTORS_PATH),
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE
                )
        ){
            ArrayList<BlockDescriptor> blocks = new ArrayList<>();

            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, blockOffset, (long) numBlocks * BlockDescriptor.BLOCK_DESCRIPTOR_ENTRY_BYTES);

            if(buffer == null)
                return null;
            for(int i = 0; i < numBlocks; i++){
                BlockDescriptor block = new BlockDescriptor();
                block.setDocidOffset(buffer.getLong());
                block.setDocidSize(buffer.getInt());
                block.setFreqOffset(buffer.getLong());
                block.setFreqSize(buffer.getInt());
                block.setMaxDocid(buffer.getInt());
                block.setNumPostings(buffer.getInt());
                blocks.add(block);

            }
            return blocks;
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String toString() {
        return  "termid=" + termid +
                ", term='" + term + '\'' +
                ", df=" + df +
                ", idf=" + idf +
                ", maxTf=" + maxTf +
                ", BM25Dl=" + BM25Dl +
                ", BM25Tf=" + BM25Tf +
                ", maxTFIDF=" + maxTFIDF +
                ", maxBM25=" + maxBM25 +
                ", docidOffset=" + docidOffset +
                ", frequencyOffset=" + frequencyOffset +
                ", docidSize=" + docidSize +
                ", frequencySize=" + frequencySize +
                ", numBlocks=" + numBlocks +
                ", blockOffset=" + blockOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VocabularyEntry that = (VocabularyEntry) o;
        return df == that.df && Double.compare(that.idf, idf) == 0 && maxTf == that.maxTf && BM25Dl == that.BM25Dl && BM25Tf == that.BM25Tf && Double.compare(that.maxTFIDF, maxTFIDF) == 0 && Double.compare(that.maxBM25, maxBM25) == 0 && docidOffset == that.docidOffset && frequencyOffset == that.frequencyOffset && docidSize == that.docidSize && frequencySize == that.frequencySize && numBlocks == that.numBlocks && blockOffset == that.blockOffset && Objects.equals(term, that.term);
    }

    protected static void setTestPaths(String test){
        if(test.equals("blockDescriptorsTest"))
            BLOCK_DESCRIPTORS_PATH = "../data/test/blockDescriptorsTest";
    }
}