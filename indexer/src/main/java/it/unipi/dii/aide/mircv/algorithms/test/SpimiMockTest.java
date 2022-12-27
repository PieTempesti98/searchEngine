package it.unipi.dii.aide.mircv.algorithms.test;


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
        PostingList pl1 = new PostingList("apple\t0:1 1:1");
        PostingList pl2 = new PostingList("apricot\t0:1");
        PostingList pl3 = new PostingList("eve\t1:1");
        PostingList pl4 = new PostingList("fruit\t0:2");
        PostingList pl5 = new PostingList("salad\t0:1");

        index.put("adam",pl);
        index.put("apple",pl1);
        index.put("apricot",pl2);
        index.put("eve",pl3);
        index.put("fruit",pl4);
        index.put("salad",pl5);


        documentIndex.put(0,new DocumentIndexEntry("document1",0,5));
        documentIndex.put(1,new DocumentIndexEntry("document2",1,3));

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