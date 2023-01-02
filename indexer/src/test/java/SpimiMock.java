
import it.unipi.dii.aide.mircv.algorithms.Spimi;
import it.unipi.dii.aide.mircv.common.beans.*;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class SpimiMock extends Spimi {

    public static HashMap<String, PostingList> executeSpimiMock(ArrayList<ProcessedDocument> testDocuments){

        HashMap<String, PostingList> index = new HashMap<>();

        int docid = 0;
        int documentLength = 0;

        for(ProcessedDocument document: testDocuments){

            for (String term : document.getTokens()) {
                PostingList posting; //posting list of a given term
                if (!index.containsKey(term)) {
                    // create new posting list if term wasn't present yet
                    posting = new PostingList(term);
                    index.put(term, posting); //add new entry (term, posting list) to entry
                } else {
                    //term is present, we can get its posting list
                    posting = index.get(term);
                }
                documentLength += document.getTokens().size();

                updateOrAddPosting(docid, posting); //insert or update new posting
                posting.updateBM25Parameters(documentLength, posting.getPostings().size());
                posting.debugSaveToDisk("mydoc","myfreq",posting.getPostings().size());
            }

            docid++;

        }

        index = index.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        return index;
    }

    public static DocumentIndex buildDocumentIndex(ArrayList<ProcessedDocument> testDocuments){

        DocumentIndex documentIndex = DocumentIndex.getInstance();

        int docid = 0;

        for(ProcessedDocument document: testDocuments){
            documentIndex.put(docid,new DocumentIndexEntry(document.getPid(),docid,document.getTokens().size()));
            docid++;
        }

        return documentIndex;

    }

    public static Vocabulary buildVocaulary(HashMap<String, PostingList> index){

        try (
                FileChannel docsFchan = (FileChannel) Files.newByteChannel(Paths.get("src/test/data/testDocumentDocids"),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                FileChannel freqsFchan = (FileChannel) Files.newByteChannel(Paths.get("src/test/data/testDocumentFreqs"),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE);
        ) {

            int numPostings = 0;
            for(PostingList pl: index.values())
                numPostings += pl.getPostings().size();

            Vocabulary vocabulary = Vocabulary.getInstance();
            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, 0, numPostings * 4L);

            // instantiation of MappedByteBuffer for integer list of freqs
            MappedByteBuffer freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, 0, numPostings * 4L);

            // check if MappedByteBuffers are correctly instantiated
            if (docsBuffer != null && freqsBuffer != null) {
                for (PostingList list : index.values()) {
                    //create vocabulary entry
                    VocabularyEntry vocEntry = new VocabularyEntry(list.getTerm());
                    vocEntry.setMemoryOffset(docsBuffer.position());
                    vocEntry.setFrequencyOffset(docsBuffer.position());

                    // write postings to file
                    for (Posting posting : list.getPostings()) {
                        // encode docid
                        docsBuffer.putInt(posting.getDocid());
                        // encode freq
                        freqsBuffer.putInt(posting.getFrequency());
                    }

                    vocEntry.updateStatistics(list);
                    vocEntry.setDocidSize((int) (numPostings * 4));
                    vocEntry.setFrequencySize((int) (numPostings * 4));
                    vocEntry.setBM25Tf(list.getBM25Tf());
                    vocEntry.setDocidSize((int) (numPostings*4));
                    vocEntry.setFrequencySize((int) (numPostings*4));

                }
            }return vocabulary;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

}
