package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;

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


public class DocumentIndexEntry implements Serializable {

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
    private static final int PID_SIZE = 64;

    /**
     * Size of the document index entry on disk
     */
    private static final int ENTRY_SIZE = PID_SIZE + 4 + 4;

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

    @Serial
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {

        stream.writeInt(docid);
        stream.writeUTF(pid);
        stream.writeInt(docLen);
    }

    @Serial
    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        docid = stream.readInt();
        pid = stream.readUTF();
        docLen = stream.readInt();

    }

    @Override
    public String toString() {
        return "DocumentIndexEntry{" +
                "pid='" + pid + '\'' +
                ", docid=" + docid +
                ", docLen=" + docLen +
                '}';
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
     * updates the document index path file (only for test purposes)
     */
    protected static void setTestPath(){
        DocumentIndexEntry.DOCINDEX_PATH = "../data/test/testDocIndex";
    }
}
