package it.unipi.dii.aide.mircv.common.beans;

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

    public PostingList(String toParse) {
        String[] termRow = toParse.split("\t");
        this.term = termRow[0];
        if(termRow.length > 1)
            parsePostings(termRow[1]);
    }

    public PostingList(){}

    /**
     * Constructor for PostingList
     * it reads the PostingList directly from file
     * @param term: vocabulary entry for the posting list to be constructed
     * @param docsPath: file path for the file containing docids of the posting list to be constructed
     * @param freqsPath: file path for the file containing freqs of the posting list to be constructed
     */
    public PostingList(VocabularyEntry term, String docsPath, String freqsPath) {
        try (FileChannel docsFChan = (FileChannel) Files.newByteChannel(
                Paths.get(docsPath),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
             FileChannel freqsFChan = (FileChannel) Files.newByteChannel(
                     Paths.get(freqsPath),
                     StandardOpenOption.WRITE,
                     StandardOpenOption.READ,
                     StandardOpenOption.CREATE)
        ) {

            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docBuffer = docsFChan.map(
                    FileChannel.MapMode.READ_ONLY,
                    term.getMemoryOffset(),
                    term.getMemorySize()
            );

            // instantiation of MappedByteBuffer for integer list of frequencies
            MappedByteBuffer freqBuffer = freqsFChan.map(
                    FileChannel.MapMode.READ_ONLY,
                    term.getFrequencyOffset(),
                    term.getMemorySize()
            );

            // create the posting list for the term
            this.term = term.getTerm();

            for (int i = 0; i < term.getDf(); i++) {
                Map.Entry<Integer, Integer> posting = new AbstractMap.SimpleEntry<>(docBuffer.getInt(), freqBuffer.getInt());
                this.getPostings().add(posting);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

    /**
     * Save to disk the posting list:
     *  - write docids in docsPath
     *  - write freqs in freqsPath
     *  - update freqs and docs offsets (this generates side effects)
     * @param docsMemOffset: offset in which to write in docids file
     * @param freqsMemOffset: offset in which to write in freqs file
     * @param docsPath: file path for file storing docids
     * @param freqsPath: file path for file storing freqs
     * @return number of bytes written
     */
    public long[] writePostingListToDisk(long docsMemOffset, long freqsMemOffset, String docsPath, String freqsPath) {
        // memory occupancy of the posting list:
        // - for each posting we have to store 2 integers (docid and freq)
        // - each integer will occupy 4 bytes since we are storing integers in byte arrays
        int numBytes = getNumBytes();

        // try to open a file channel to the file of the inverted index
        try (FileChannel docsFchan = (FileChannel) Files.newByteChannel(Paths.get(docsPath), StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE);
             FileChannel freqsFchan = (FileChannel) Files.newByteChannel(Paths.get(freqsPath), StandardOpenOption.WRITE,
                     StandardOpenOption.READ, StandardOpenOption.CREATE)
        ) {

            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, docsMemOffset, numBytes/2);

            // instantiation of MappedByteBuffer for integer list of freqs
            MappedByteBuffer freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, freqsMemOffset, numBytes/2);

            // check if MappedByteBuffers are correctly instantiated
            if (docsBuffer != null && freqsBuffer != null) {
                // write postings to file
                for (Map.Entry<Integer, Integer> posting : postings) {
                    // encode docid
                    docsBuffer.putInt(posting.getKey());
                    // encode freq
                    freqsBuffer.putInt(posting.getValue());
                }
                //return updated buffer positions
                return new long[]{docsMemOffset + docsBuffer.position(), freqsMemOffset + docsBuffer.position()};
            }
        } catch (InvalidPathException e) {
            System.out.println("Path Error " + e);
        } catch (IOException e) {
            System.out.println("I/O Error " + e);
        }
        return null;
    }
}
