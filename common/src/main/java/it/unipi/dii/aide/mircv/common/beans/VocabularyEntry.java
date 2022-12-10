package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;


/**
 * Entry of the vocabulary for a term
 */
public class VocabularyEntry implements Serializable {


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

    /**
     * Document frequency of the term
     */
    private int df = 0;

    /**
     * term frequency of the term in the whole collection
     */
    private int tf = 0;

    /**
     * inverse of document frequency of the term
     */
    private double idf = 0;

    /**
     * starting point of the term's posting list in the inverted index in bytes
     */
    private long memoryOffset = 0;

    /**
     * Starting point of the frequencies in the inverted index in bytes
     */
    private long frequencyOffset = 0;

    /**
     * size of the term's posting list in the inverted index in bytes
     */
    private long memorySize = 0;

    /*
     size of the term
     */

    private final int TERM_SIZE = 32;

    /**
     * term size + 4 + 4 + 8 + 8 + 8 + 8
     * we have to store 2 int, 1 double and 3 longs
     */
    private final long ENTRY_SIZE = 32 + 4 + 4 + 8 + 8 + 8 + 8;

    /**
     * Constructor for the vocabulary entry for the term passed as parameter
     * Assign the termid to the term and initializes all the statistics and memory information
     * @param term the token of the entry
     */

    public VocabularyEntry(){}

    public VocabularyEntry(String term){

        // Assign the term
        this.term = term;

        // Assign the termid and increase the counter
        this.termid = termCount;
        termCount ++;
    }

    /**
     * Updates the statistics of the vocabulary:
     * updates tf and df with the data of the partial posting list processed
     * @param list the posting list from which the method computes the statistics
     */
    public void updateStatistics(PostingList list){

        //for each element of the intermediate posting list
        for(Map.Entry<Integer, Integer> posting: list.getPostings()){

            // update the term frequency
            this.tf += posting.getValue();

            // update the raw document frequency
            this.df++;
        }
    }

    /**
     * Compute the idf using the values computed during the merging of the indexes
     */
    public void computeIDF(){
        this.idf = Math.log10(CollectionStatistics.getNumDocuments()/(double)this.df);
    }

    public void computeIDF(int numDocuments){
        this.idf = Math.log10(numDocuments/(double)this.df);
    }

    /**
     * Returns the vocabulary entry as a string formatted in the following way:
     * [termid]-[term]-[idf] [tf] [memoryOffset] [memorySize]\n
     * @return the formatted string
     */
    public String toString(){
        //format the string for the vocabulary entry
        return term + "->" +
                        tf + " " + df + " "+
                        idf + " " +
                        memoryOffset + " " + frequencyOffset + " " +
                        memorySize +
                        '\n';
    }

    public void setMemorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    public void setMemoryOffset(long memoryOffset) {this.memoryOffset = memoryOffset;
    }

    public void setFrequencyOffset(long freqOffset) {
        this.frequencyOffset = freqOffset;
    }

    public int getTERM_SIZE() {return TERM_SIZE;}

    public long getENTRY_SIZE() {return ENTRY_SIZE;}

    @Serial
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {

        stream.writeInt(termid);
        stream.writeUTF(term);
        stream.writeInt(tf);
        stream.writeInt(df);
        stream.writeDouble(idf);
        stream.writeLong(memoryOffset);
        stream.writeLong(frequencyOffset);
        stream.writeLong(memorySize);
    }

    @Serial
    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        termid = stream.readInt();
        term = stream.readUTF();
        tf = stream.readInt();
        df = stream.readInt();
        idf = stream.readDouble();
        memoryOffset = stream.readLong();
        frequencyOffset = stream.readLong();
        memorySize = stream.readLong();

    }

    public long write_entry_to_disk(long position, String PATH){
        try (FileChannel fChan = (FileChannel) Files.newByteChannel(
                Paths.get(PATH),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ)) {


            // instantiation of MappedByteBuffer
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

            // Write the document frequency into file
            buffer.putInt(this.getDf());

            // Write the term frequency into file
            buffer.putInt(this.getTf());

            //wirte IDF into file
            buffer.putDouble(this.getIdf());

            //write memory offset into file
            buffer.putLong(this.getMemoryOffset());

            //write frequency offset into file
            buffer.putLong(this.getFrequencyOffset());

            //write memory offset into file
            buffer.putLong(this.getMemorySize());


            // update position for which we have to start writing on file
            position += ENTRY_SIZE;
            return position;



        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

/**
 * Read the document index entry from disk
 * @param memoryOffset the memory offset from which we start reading
 * @return true if the read is successful
*/
public boolean readFromDisk(long memoryOffset){
// try to open a file channel to the file of the inverted index
try (FileChannel fChan = (FileChannel) Files.newByteChannel(
    Paths.get("data/partial_vocabulary/vocabulary_0"),
    StandardOpenOption.WRITE,
    StandardOpenOption.READ,
    StandardOpenOption.CREATE)) {

    // instantiation of MappedByteBuffer for the PID read
    MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memoryOffset, ENTRY_SIZE);

    // Buffer not created
    if(buffer == null)
        return false;

    // Read from file into the charBuffer, then pass to the string
    CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

    if(charBuffer.length() == 0)
        return false;

    this.term = charBuffer.toString().split("\0")[0];

    if(term == null)
        return false;

    // Instantiate the buffer for reading other information
    buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memoryOffset + TERM_SIZE, ENTRY_SIZE - TERM_SIZE);

    // Buffer not created
    if(buffer == null)
        return false;

    // read the docid
    this.df = buffer.getInt();
    // read the doclen
    this.tf = buffer.getInt();

    this.idf = buffer.getDouble();

    this.memoryOffset = buffer.getLong();

    this.frequencyOffset = buffer.getLong();

    this.memorySize = buffer.getLong();


    return true;

    }catch(Exception e){
        e.printStackTrace();
        return false;
    }
}

    public String getTerm() {
        return term;
    }

    public int getDf() {
        return df;
    }

    public int getTf() {
        return tf;
    }

    public double getIdf() {
        return idf;
    }

    public long getMemoryOffset() {
        return memoryOffset;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public long getFrequencyOffset() {
        return frequencyOffset;
    }

}
