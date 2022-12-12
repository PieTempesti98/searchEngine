package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;

public class PostingList implements Serializable{

    private String term;
    private ArrayList<Map.Entry<Integer, Integer>> postings = new ArrayList<>();
    private final static String PATH_TO_INVERTED_INDEX = ConfigurationParameters.getInvertedIndexPath();

    public PostingList(String toParse) {
        String[] termRow = toParse.split("\t");
        this.term = termRow[0];
        if(termRow.length > 1)
            parsePostings(termRow[1]);
    }

    public PostingList(){}
    private void parsePostings(String rawPostings){
        String[] documents = rawPostings.split(" ");
        for(String elem: documents){
            String[] posting = elem.split(":");
            postings.add(new AbstractMap.SimpleEntry<>(Integer.parseInt(posting[0]), Integer.parseInt(posting[1])));
        }
    }

    public String getTerm() {
        return term;
    }

    public ArrayList<Map.Entry<Integer, Integer>> getPostings() {
        return postings;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void appendPostings(ArrayList<Map.Entry<Integer, Integer>> newPostings){
        postings.addAll(newPostings);
    }

    /**
     * save to disk the posting list as a 2 byte arrays (first for docids, second for freqs)
     * @param memoryOffset the memory offset (in the inverted index file) at which the posting list will be stored
     */
    public int saveToDisk(long memoryOffset, VocabularyEntry vocabularyEntry) {
        // memory occupancy of the posting list:
        // - for each posting we have to store 2 integers (docid and freq)
        // - each integer will occupy 4 bytes since we are storing integers in byte arrays
        int numBytes = getNumBytes();

        // try to open a file channel to the file of the inverted index
        try (FileChannel fChan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_INVERTED_INDEX), StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE)){

            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memoryOffset, numBytes);

            // check if MappedByteBuffers are correctly instantiated
            if (buffer != null) {

                // write postings to file
                for (Map.Entry<Integer, Integer> posting : postings) {
                    // encode docid
                    buffer.putInt(posting.getKey());
                }
                long freqOffset = buffer.position();

                // set the frequency offset in the vocabulary
                vocabularyEntry.setFrequencyOffset(freqOffset);

                for (Map.Entry<Integer, Integer> posting : postings) {
                    // encode docid
                    buffer.putInt(posting.getValue());
                }
                return numBytes;
            }
        } catch (InvalidPathException e) {
            System.out.println("Path Error " + e);
        } catch (IOException e) {
            System.out.println("I/O Error " + e);
        }
        return -1;
    }

    /**
     * method to return the numbers of bytes occupied by this posting list when stored in memory
     * @return posting list's space occupancy in bytes
     */
    public int getNumBytes() {
        return postings.size()*4*2;
    }
    @Override
    public String toString() {
        return "PostingList{" +
                "term='" + term + '\'' +
                ", postings=" + postings +
                '}';
    }

    @Serial
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {

        stream.writeUTF(term);
        stream.writeObject(postings);
    }

    @Serial
    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        term = stream.readUTF();
        postings = (ArrayList<Map.Entry<Integer, Integer>>) stream.readObject();

    }

    public long writePostingListToDisk(long docsMemOffset, long freqsMemOffset, String docsPath, String freqsPath) {
        return getNumBytes();
    }
}
