import it.unipi.dii.aide.mircv.common.beans.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SpimiMockTest {


    private static HashMap<String, PostingList> index = new LinkedHashMap<>();
    private static ArrayList<ProcessedDocument> testDocuments = new ArrayList<>();
    private static  DocumentIndex documentIndex = DocumentIndex.getInstance();
    private static Vocabulary vocabulary = Vocabulary.getInstance();

    @BeforeAll
    public static void init(){

        ProcessedDocument d1 = new ProcessedDocument();
        d1.setPid("document1");
        ArrayList<String> tokens1 = new ArrayList<>(Arrays.asList("fruit","apricot","apple","fruit","salad"));
        d1.setTokens(tokens1);
        testDocuments.add(d1);

        ProcessedDocument d2 = new ProcessedDocument();
        d2.setPid("document2");
        ArrayList<String> tokens2 = new ArrayList<>(Arrays.asList("apple","adam","eve"));
        d2.setTokens(tokens2);
        testDocuments.add(d2);


        PostingList pl = new PostingList("adam\t1:1");
        pl.setBM25Dl(5);
        pl.setBM25Tf(1);

        PostingList pl1 = new PostingList("apple\t0:1 1:1");
        pl.setBM25Dl(5);
        pl.setBM25Tf(1);

        PostingList pl2 = new PostingList("apricot\t0:1");
        pl.setBM25Dl(5);
        pl.setBM25Tf(1);

        PostingList pl3 = new PostingList("eve\t1:1");
        pl.setBM25Dl(3);
        pl.setBM25Tf(1);

        PostingList pl4 = new PostingList("fruit\t0:2");
        pl.setBM25Dl(5);
        pl.setBM25Tf(2);

        PostingList pl5 = new PostingList("salad\t0:1");
        pl.setBM25Dl(5);
        pl.setBM25Tf(1);

        index.put("adam",pl);
        index.put("apple",pl1);
        index.put("apricot",pl2);
        index.put("eve",pl3);
        index.put("fruit",pl4);
        index.put("salad",pl5);


        documentIndex.put(0,new DocumentIndexEntry("document1",0,5));
        documentIndex.put(1,new DocumentIndexEntry("document2",1,3));

        VocabularyEntry e = new VocabularyEntry("adam");
        e.setDf(1);
        e.setMemoryOffset(0);
        e.setFrequencyOffset(0);
        e.setMaxTf(1);
        e.setBM25Dl(5);
        e.setDocidSize(4);
        e.setFrequencySize(4);

        VocabularyEntry e1 = new VocabularyEntry("apple");
        e.setDf(1);
        e.setMemoryOffset(140);
        e.setFrequencyOffset(140);
        e.setMaxTf(1);
        e.setBM25Dl(5);
        e.setDocidSize(4);
        e.setFrequencySize(4);

        VocabularyEntry e2 = new VocabularyEntry("apricot");
        e.setDf(1);
        e.setMemoryOffset(280);
        e.setFrequencyOffset(280);
        e.setMaxTf(1);
        e.setBM25Dl(5);
        e.setDocidSize(8);
        e.setFrequencySize(8);

        VocabularyEntry e3 = new VocabularyEntry("eve");
        e.setDf(1);
        e.setMemoryOffset(420);
        e.setFrequencyOffset(420);
        e.setMaxTf(1);
        e.setBM25Dl(3);
        e.setDocidSize(4);
        e.setFrequencySize(4);

        VocabularyEntry e4 = new VocabularyEntry("fruit");
        e.setDf(1);
        e.setMemoryOffset(560);
        e.setFrequencyOffset(560);
        e.setMaxTf(2);
        e.setBM25Dl(5);
        e.setDocidSize(4);
        e.setFrequencySize(4);

        VocabularyEntry e5 = new VocabularyEntry("salad");
        e.setDf(1);
        e.setMemoryOffset(700);
        e.setFrequencyOffset(700);
        e.setMaxTf(1);
        e.setBM25Dl(5);
        e.setDocidSize(4);
        e.setFrequencySize(4);


        vocabulary.put("adam",e);
        vocabulary.put("apple",e1);
        vocabulary.put("apricot",e2);
        vocabulary.put("eve",e3);
        vocabulary.put("fruit",e4);
        vocabulary.put("salad",e5);

    }


    @Test
    void buildDocumentIndex_ShouldBeEqual() {

        assertEquals(documentIndex, SpimiMock.buildDocumentIndex(testDocuments));
    }

    @Test
    public void buildVocabulary_ShouldbeEqual() {

        assertEquals(vocabulary, SpimiMock.buildVocaulary(index));
    }

    @Test
    public void buildIndex_ShouldBeEqual() {

        assertEquals(index.toString(), SpimiMock.executeSpimiMock(testDocuments).toString());
    }



}