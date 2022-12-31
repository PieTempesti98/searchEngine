package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;

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


public class DocumentIndexEntry{

    /**
     * pid of a document
     * */
    private String pid;

    /**
    * docid of a document
    * */
    private int docid;

    /**
     * length of a documents in terms of number of terms
     * */
    private int docLen;

    /**
     * Memory offset of the documentIndex file
     */
    private static long memOffset = 0;

    /**
     * Size of the pid on disk
     */
    public static final int PID_SIZE = 64;

    /**
     * Size of the document index entry on disk
     */
    public static final int ENTRY_SIZE = PID_SIZE + 4 + 4;

    /**
     * Path to the documentIndex file
     */
    private static String DOCINDEX_PATH = ConfigurationParameters.getDocumentIndexPath();

    /**
     * Default constructor with 0 parameters
     */
    public DocumentIndexEntry(){}
    /**
     * Constructor for the document index entry of a specific document
     * @param pid the pid of such document
     * @param docid the docid of such documents
     * @param docLen the length of such documents in terms of number of terms
     */
    public DocumentIndexEntry(String pid, int docid, int docLen) {
        this.pid = pid;
        this.docid = docid;
        this.docLen = docLen;
    }

    public String getPid() {return pid;}

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getDocid() {
        return docid;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public int getDocLen() {
        return docLen;
    }

    public void setDocLen(int docLen) {
        this.docLen = docLen;
    }

    @Override
    public String toString() {
        return "Document: " + docid + " -> " +
                "pid = " + pid +
                ", document length = " + docLen;
    }

    /**
     * Write the document index entry on disk
     * @return the offset of the entry
     */
    public long writeToDisk(){
        // try to open a file channel to the file of the inverted index
        try (FileChannel fChan = (FileChannel) Files.newByteChannel(
                Paths.get(DOCINDEX_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE)) {
            // instantiation of MappedByteBuffer for the entry
            MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memOffset, ENTRY_SIZE);

            // Buffer not created
            if(buffer == null)
                return -1;

            // Create the CharBuffer with size = PID_SIZE
            CharBuffer charBuffer = CharBuffer.allocate(PID_SIZE);
            for(int i = 0; i < this.pid.length(); i++)
                charBuffer.put(i, this.pid.charAt(i));
            // Write the PID into file
            buffer.put(StandardCharsets.UTF_8.encode(charBuffer));

            // Write the docid into file
            buffer.putInt(this.docid);
            // Write the doclen into file
            buffer.putInt(this.docLen);

            // save the start offset of the structure
            long startOffset = memOffset;
            // update memory offset
            memOffset = memOffset + ENTRY_SIZE;


            return startOffset;

        }catch(Exception e){
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
                Paths.get(DOCINDEX_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE)) {

            // instantiation of MappedByteBuffer for the PID read
            MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memoryOffset, PID_SIZE);

            // Buffer not created
            if(buffer == null)
                return false;

            // Read from file into the charBuffer, then pass to the string
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
            if(charBuffer.toString().split("\0").length == 0)
                return true;
            this.pid = charBuffer.toString().split("\0")[0];

            // Instantiate the buffer for reading other information
            buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memoryOffset + PID_SIZE, ENTRY_SIZE - PID_SIZE);

            // Buffer not created
            if(buffer == null)
                return false;

            // read the docid
            this.docid = buffer.getInt();
            // read the doclen
            this.docLen = buffer.getInt();

            return true;

        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @return the size of an entry
     */
    public static int getEntrySize(){return ENTRY_SIZE;}


    /**
     * updates the document index path file (only for test purposes)
     */
    protected static void setTestPath(){
        DocumentIndexEntry.DOCINDEX_PATH = "../data/test/testDocIndex";
        DocumentIndexEntry.memOffset = 0;
    }

    public static void setDocindexPath(String path ){
        DocumentIndexEntry.DOCINDEX_PATH = path;
    }


    /**
     * function to write a summarization of the most important data about a document index entry as plain text in the debug file
     * @param path: path of the file where to write
     */
    public void debugWriteToDisk(String path) {
        FileUtils.createDirectory("data/debug");
        FileUtils.createIfNotExists("data/debug/"+path);

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("data/debug/"+path, true));
            writer.write(this.toString()+"\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean equals(Object o){

        if(o == this)
            return true;

        if (!(o instanceof DocumentIndexEntry de)) {
            return false;
        }

        return de.getDocid() == this.getDocid() && de.getPid().equals(this.getPid()) && de.getDocLen() == this.getDocLen();
    }

    public static void resetOffset(){
        memOffset = 0;
    }



}
