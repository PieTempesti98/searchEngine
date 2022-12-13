package indexLoading;

import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.Map;

/**
 * Class used to load in main memory the posting lists of the query terms
 */
public class IndexLoader {

    /**
     * Load from the stored inverted index the posting list of a term
     *
     * @param term The vocabulary entry of the term
     * @return the posting list of that term
     */
    public static PostingList loadTerm(VocabularyEntry term) {
        try (FileChannel docidChan = (FileChannel) Files.newByteChannel(
                Paths.get(ConfigurationParameters.getInvertedIndexDocs()),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
             FileChannel freqChan = (FileChannel) Files.newByteChannel(
                     Paths.get(ConfigurationParameters.getInvertedIndexFreqs()),
                     StandardOpenOption.WRITE,
                     StandardOpenOption.READ,
                     StandardOpenOption.CREATE)   ) {

            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docBuffer = docidChan.map(
                    FileChannel.MapMode.READ_ONLY,
                    term.getMemoryOffset(),
                    term.getMemorySize()
            );

            // instantiation of MappedByteBuffer for integer list of frequencies
            MappedByteBuffer freqBuffer = freqChan.map(
                    FileChannel.MapMode.READ_ONLY,
                    term.getFrequencyOffset(),
                    term.getMemorySize()
            );

            // create the posting list for the term
            PostingList postingList = new PostingList(term.getTerm());

            for (int i = 0; i < term.getDf(); i++) {

                Map.Entry<Integer, Integer> posting = new AbstractMap.SimpleEntry<>(docBuffer.getInt(), freqBuffer.getInt());
                postingList.getPostings().add(posting);
            }
            return postingList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
