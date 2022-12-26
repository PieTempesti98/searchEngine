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

    private static long memoryOffset = 0;

    public static long getMemoryOffset() {
        return memoryOffset;
    }

    public long getDocidOffset() {
        return docidOffset;
    }

    public void setDocidOffset(long docidOffset) {
        this.docidOffset = docidOffset;
    }

    public int getDocidSize() {
        return docidSize;
    }

    public void setDocidSize(int docidSize) {
        this.docidSize = docidSize;
    }

    public long getFreqOffset() {
        return freqOffset;
    }

    public void setFreqOffset(long freqOffset) {
        this.freqOffset = freqOffset;
    }

    public int getFreqSize() {
        return freqSize;
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

    public int getNumPostings() {
        return numPostings;
    }

    public void setNumPostings(int numPostings) {
        this.numPostings = numPostings;
    }

    public boolean saveDescriptorOnDisk(FileChannel fChan){
        try {
            MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memoryOffset, BLOCK_DESCRIPTOR_ENTRY_BYTES);

            if(buffer != null){
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
            FileChannel docsFChan = (FileChannel) Files.newByteChannel(Paths.get(ConfigurationParameters.getInvertedIndexDocs()),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE
            );
            FileChannel freqsFChan = (FileChannel) Files.newByteChannel(Paths.get(ConfigurationParameters.getInvertedIndexFreqs()),
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
                int[] decompressedFreqs = UnaryCompressor.integerArrayDecompression(compressedDocids, numPostings);

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

}
