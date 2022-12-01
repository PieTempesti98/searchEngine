package it.unipi.dii.aide.mircv.algorithms.test;

import it.unipi.dii.aide.mircv.algorithms.Spimi;
import it.unipi.dii.aide.mircv.common.beans.DocumentIndexEntry;
import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;

import java.util.ArrayList;
import java.util.HashMap;


public class SpimiMock extends Spimi {

    public static HashMap<String, PostingList> executeSpimiMock(ArrayList<ProcessedDocument> testDocuments){

        HashMap<String, PostingList> index = new HashMap<>();

        int docid = 0;

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

                //updateOrAddPosting(docid, posting); //insert or update new posting
            }

            docid++;
        }

        return index;
    }

    public static HashMap<Integer,DocumentIndexEntry> buildDocumentIndex(ArrayList<ProcessedDocument> testDocuments){

        HashMap<Integer,DocumentIndexEntry> documentIndex = new HashMap<>();

        int docid = 0;

        for(ProcessedDocument document: testDocuments){
            docid++;
            documentIndex.put(docid,new DocumentIndexEntry(document.getPid(),docid,document.getTokens().size()));
        }

        return documentIndex;

    }
}
