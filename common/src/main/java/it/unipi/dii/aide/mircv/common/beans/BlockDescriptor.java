package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;

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
     * Method to retrieve the postings in this block
     * @return the list of postings in the block
     */
    public ArrayList<Posting> getBlockPostings(){
        try(
            FileChannel docsFchan = (FileChannel) Files.newByteChannel(Paths.get(ConfigurationParameters.getInvertedIndexDocs()),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE
            );
            FileChannel freqsFchan = (FileChannel) Files.newByteChannel(Paths.get(ConfigurationParameters.getInvertedIndexFreqs()),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);
        ){
            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docBuffer = docsFchan.map(
                    FileChannel.MapMode.READ_ONLY,
                    docidOffset,
                    docidSize
            );

            // instantiation of MappedByteBuffer for integer list of frequencies
            MappedByteBuffer freqBuffer = freqsFchan.map(
                    FileChannel.MapMode.READ_ONLY,
                    freqOffset,
                    freqSize
            );

            ArrayList<Posting> block = new ArrayList<>();

            // for each posting read from the files and append the new posting in the list
            for (int i = 0; i < numPostings; i++) {
                Posting posting = new Posting(docBuffer.getInt(), freqBuffer.getInt());
                block.add(posting);
            }
            return block;


        }catch(Exception e){
            e. printStackTrace();
            return null;
        }

    }
}
