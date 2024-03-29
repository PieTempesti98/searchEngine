package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.compression.UnaryCompressor;
import it.unipi.dii.aide.mircv.common.compression.VariableByteCompressor;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.config.Flags;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

/**
 * Descriptor of a block of postings in a posting list, used to implement the skipping
 */
public class BlockDescriptor {

    /**
     * starting byte of the block in the docid file of inverted index
     */
    private long docidOffset;

    /**
     * byte size of the block in the docid file of the inverted index
     */
    private int docidSize;

    /**
     * starting byte of the block in the frequency file of inverted index
     */
    private long freqOffset;

    /**
     * byte size of the block in the frequency file of the inverted index
     */
    private int freqSize;

    /**
     * max docid in the block
     */
    private int maxDocid;

    /**
     * number of terms in the block
     */
    private int numPostings;

    /**
     * # of bytes on disk for a block descriptor entry: 4 int + 2 long = 32 bytes
     */
    public static final int BLOCK_DESCRIPTOR_ENTRY_BYTES = 4 * 4 + 2 * 8;

    /**
     * memory offset reached while writing the block descriptor file
     */
    private static long memoryOffset = 0;

    /**
     * path to the docid file of the inverted index
     */
    private static String INVERTED_INDEX_DOCS = ConfigurationParameters.getInvertedIndexDocs();
    /**
     * path to the frequency file of the inverted index
     */
    private static String INVERTED_INDEX_FREQS = ConfigurationParameters.getInvertedIndexFreqs();

    public static long getMemoryOffset() {
        return memoryOffset;
    }

    public void setDocidOffset(long docidOffset) {
        this.docidOffset = docidOffset;
    }

    public void setDocidSize(int docidSize) {
        this.docidSize = docidSize;
    }

    public void setFreqOffset(long freqOffset) {
        this.freqOffset = freqOffset;
    }

    public void setFreqSize(int freqSize) {
        this.freqSize = freqSize;
    }

    public int getMaxDocid() {
        return maxDocid;
    }

    public void setMaxDocid(int maxDocid) {
        this.maxDocid = maxDocid;
    }

    public void setNumPostings(int numPostings) {
        this.numPostings = numPostings;
    }

    /**
     * method that saves on file the block descriptor
     *
     * @param fChan the file channel of the block descriptor file
     * @return true if the storing was successful
     */
    public boolean saveDescriptorOnDisk(FileChannel fChan) {
        try {
            MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memoryOffset, BLOCK_DESCRIPTOR_ENTRY_BYTES);

            if (buffer != null) {
                buffer.putLong(docidOffset);
                buffer.putInt(docidSize);
                buffer.putLong(freqOffset);
                buffer.putInt(freqSize);
                buffer.putInt(maxDocid);
                buffer.putInt(numPostings);

                memoryOffset += BLOCK_DESCRIPTOR_ENTRY_BYTES;

                return true;
            }
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    /**
     * method to get block's postings from file using compressed mode or not
     * @return arraylist containing block's postings
     */
    public ArrayList<Posting> getBlockPostings(){
        try(
            FileChannel docsFChan = (FileChannel) Files.newByteChannel(Paths.get(INVERTED_INDEX_DOCS),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE
            );
            FileChannel freqsFChan = (FileChannel) Files.newByteChannel(Paths.get(INVERTED_INDEX_FREQS),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);
        ){
            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docBuffer = docsFChan.map(
                    FileChannel.MapMode.READ_ONLY,
                    docidOffset,
                    docidSize
            );
            MappedByteBuffer freqBuffer = freqsFChan.map(
                    FileChannel.MapMode.READ_ONLY,
                    freqOffset,
                    freqSize
            );

            if(docBuffer ==null || freqBuffer == null){
                return null;
            }

            ArrayList<Posting> block = new ArrayList<>();

            if(Flags.isCompressionEnabled()){
                // initialization of arrays of bytes for docids and freqs (compressed)
                byte[] compressedDocids = new byte[docidSize];
                byte[] compressedFreqs = new byte[freqSize];

                // read bytes from file
                docBuffer.get(compressedDocids, 0, docidSize);
                freqBuffer.get(compressedFreqs, 0, freqSize);

                // perform decompression of docids and frequencies
                int[] decompressedDocids = VariableByteCompressor.integerArrayDecompression(compressedDocids, numPostings);
                int[] decompressedFreqs = UnaryCompressor.integerArrayDecompression(compressedFreqs, numPostings);

                // populate the array list of postings with the decompressed information about block postings
                for(int i=0; i<numPostings; i++){
                    Posting posting = new Posting(decompressedDocids[i], decompressedFreqs[i]);
                    block.add(posting);
                }
            }
            else {
                // not compressed posting list
                for(int i = 0; i < numPostings; i++){
                    // create a new posting reading docid and frequency from the buffers
                    Posting posting = new Posting(docBuffer.getInt(), freqBuffer.getInt());
                    block.add(posting);
                }

            }

            return block;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String toString() {
        return "Block info : " +
                "docidOffset = " + docidOffset +
                ", docidSize = " + docidSize +
                ", freqOffset = " + freqOffset +
                ", freqSize = " + freqSize +
                ", maxDocid = " + maxDocid +
                ", numPostings = " + numPostings;
    }
    
    /** needed for testing purposes
     * @param invertedIndexDocs: path to be set
     */
    public static void setInvertedIndexDocs(String invertedIndexDocs) {
        INVERTED_INDEX_DOCS = invertedIndexDocs;
    }

    /** needed for testing purposes
     * @param invertedIndexFreqs: path to be set
     */
    public static void setInvertedIndexFreqs(String invertedIndexFreqs) {
        INVERTED_INDEX_FREQS = invertedIndexFreqs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlockDescriptor that = (BlockDescriptor) o;
        return docidOffset == that.docidOffset && docidSize == that.docidSize && freqOffset == that.freqOffset && freqSize == that.freqSize && maxDocid == that.maxDocid && numPostings == that.numPostings;
    }

    /** needed for testing purposes
     * @param memoryOffset: offest to be set
     */
    public static void setMemoryOffset(long memoryOffset) {
        BlockDescriptor.memoryOffset = memoryOffset;
    }

}
