package indexLoading;

import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;
import it.unipi.dii.aide.mircv.common.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;

public class IndexLoader {

    /**
     * Load from the stored inverted index the posting list of a term
     * @param term The vocabulary entry of the term
     * @return the posting list of that term
     */
    public static PostingList loadTerm(VocabularyEntry term){
        try (FileChannel fChan = (FileChannel) Files.newByteChannel(Paths.get(ConfigurationParameters.getInvertedIndexPath()), StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE)) {

            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docBuffer = fChan.map(FileChannel.MapMode.READ_ONLY, term.getMemoryOffset(), term.getMemorySize());

            // instantiation of MappedByteBuffer for integer list of frequencies
            MappedByteBuffer freqBuffer = fChan.map(FileChannel.MapMode.READ_ONLY, term.getFrequencyOffset(), term.getMemorySize());

            // create the posting list for the term
            PostingList postingList = new PostingList(term.getTerm());
            for (int i = 0; i < term.getDf(); i++) {

                Map.Entry<Integer, Integer> posting = new AbstractMap.SimpleEntry<>(docBuffer.getInt(), freqBuffer.getInt());
                postingList.getPostings().add(posting);
            }
            return postingList;
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }

}
