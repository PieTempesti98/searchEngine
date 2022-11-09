package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.beans.PostingList;
import it.unipi.dii.aide.mircv.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.utils.Utility;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Merger {

    private static final ArrayList<BufferedReader> buffers = new ArrayList<>();

    private static final ArrayList<String> nextTerm = new ArrayList<>();

    private static final ArrayList<PostingList> nextPostingList = new ArrayList<>();

    private static final String INTERMEDIATE_INDEX_PATH = "data/index_";

    private static int openIndexes;

    private static ArrayList<VocabularyEntry> vocabulary = new ArrayList<>();

    private static void initialize() throws Exception{
        openIndexes = Utility.getNumIndexes();

        for(int i = 0; i < openIndexes; i++){
            String path = INTERMEDIATE_INDEX_PATH + i + ".txt";
            BufferedReader buffer = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8);
            String line = buffer.readLine();

            // If the buffer is emptu we add null in its position
            if(line == null){
                buffers.add(null);
                nextTerm.add(null);
                nextPostingList.add(null);
                continue;
            }

            PostingList list = new PostingList(line);

            buffers.add(buffer);
            nextTerm.add(list.getTerm());
            nextPostingList.add(list);
        }
    }

    private static void readBufferLine(int i){
        /*TODO: implement the method [Pietro]
        *  read the new buffer line
        *  if it is null close the buffer and set null values
        *  else update the lists with the new value
        *  Note: reuse the code from the initialize() method */


    }

    private static String getMinTerm(){
        String term = nextTerm.get(0);
        for(String elem: nextTerm){
            if(elem.compareTo(term) < 0){
                term = elem;
            }
        }
        return term;
    }

    public static boolean mergeIndexes(){


        try{
            initialize();

            while(openIndexes > 0){
                String termToProcess = getMinTerm();
                PostingList finalList = new PostingList();
                finalList.setTerm(termToProcess);

                // TODO: fix the instantiation once the class is defined [Pietro]
                VocabularyEntry vocabularyEntry = new VocabularyEntry();
                for(int i = 0; i < Utility.getNumIndexes(); i++){
                    // Found the matching term
                    if(nextTerm.get(i) != null && nextTerm.get(i).equals(termToProcess)){

                        PostingList list = nextPostingList.get(i);
                        // Append the posting list to the final posting list of the term
                        finalList.appendPostings(list.getPostings());

                        //Update the lists for the scanned index
                        readBufferLine(i);

                        //update vocabulary statistics
                        vocabularyEntry.updateStatistics(list);
                    }
                }
                //the list for the term is computed, save it on disk and compute the information to store in vocabulary
                finalList.saveToDisk();

                vocabularyEntry.computeMemoryOffsets();
                vocabularyEntry.saveToDisk();
            }
        }catch(Exception e){
            e.printStackTrace();
        }

       return false;
    }

}
