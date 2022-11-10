package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;
import org.apache.commons.lang3.tuple.MutablePair;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;


public class Spimi {

    //path to the file on the disk storing the processed collection
    private static final String PATH_TO_DOCUMENTS = ConfigurationParameters.getProcessedCollectionPath();

    //path to the file on the disk storing the partial indexes
    private static final String PATH_BASE_TO_INDEX = ConfigurationParameters.getPartialIndexPath();

    //path to the file on the disk storing the document index
    private static final String PATH_TO_DOCUMENT_INDEX = ConfigurationParameters.getDocumentIndexPath();

    //chunk of memory to be kept free
    //private static final long MEMORY_TRESHOLD = 1048576; //1MB
    private static final long MEMORY_TRESHOLD = 106099200;

    //structure storing the partial inverted index
    private static HashMap<String, ArrayList<MutablePair<Integer, Integer>>> index = new HashMap<>();

    //counts the number of partial indexes created
    private static int num_index = 0;


    /**
     *
     * @param path: path to file on disk
     *
     *            Flushes partial index onto disk and cleans
     *            index data structure such that it can be reused
     * */
    private static void save_index_to_disk(String path){

        System.out.println("writing to " + path);

        if(index.isEmpty()) //if the index is empty there is nothing to write on disk
            return;

        FileUtils.createOrCleanFile(path); //create new file on which index will be saved

        StringBuilder entry = new StringBuilder(); //create new file entry

        //sort index in lexicographic order
        index = index.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        ArrayList<MutablePair<Integer, Integer>> postingList;
        for(String key :index.keySet()) {  //for each posting list in the inverted index
            entry.append(key).append("\t"); //initialize the entry to write on the file
            postingList = index.get(key); //get posting list from index
            postingList.sort(Comparator.comparing(MutablePair::getLeft)); //sort posting lists in ascending order of docid

            //create entry in this format for each term    term \t docid1,freq1 docid2,freq2 ... docidN,freqN \n
            for(int i = 0; i < postingList.size() - 1; i++){
                entry.append(postingList.get(i).getLeft()).append(",").append(postingList.get(i).getRight()).append(" ");
            }

            //append last posting
            entry.append(postingList.get(postingList.size() - 1).getLeft()).append(",").append(postingList.get(postingList.size() - 1).getRight()).append("\n");
            writeEntry(path, entry.toString()); //write entry on file
            entry.setLength(0); // clear entry to start over with new term
        }

        index.clear(); //empty index
        num_index ++; //update number of partial inverted indexes
    }

    /**
    * @param path: path to file on disk
     * @param entry: entry to write on disk
     *
     *             Utility function writing an entry onto disk
    *
    * **/
    private static void writeEntry(String path,String entry){
        try {
            Files.writeString(Paths.get(path), entry, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * @param docid: docid of a certain document
     * @param postingList: posting list of a given term
     *
     *                   Function that searched for a given docid in a posting list.
     *                   If the document is already present it updates the term frequency for that
     *                   specific document, if that's not the case creates a new Pair(docid,freq)
     *                   in which frequency is set to 1 and adds this pair to the posting list
     * **/
    private static void updateOrAddPosting(int docid, ArrayList<MutablePair<Integer, Integer>> postingList){
        boolean found = false;
        for(MutablePair<Integer, Integer> posting: postingList){ //iterate for each posting in postinglist

            if(docid == posting.getLeft()){ //docid found
                posting.setRight(posting.getRight() + 1); //update frequency
                found = true;
            }
        }

        if(!found) //document with that docid wasn't int the posting list
            postingList.add(new MutablePair<>(docid,1)); //create new pair and add it

    }


    /**
     * @param term: a string describing a word
     *      return the posting list of the given term
     * **/
    private static ArrayList<MutablePair<Integer, Integer>> getPostingList(String term){
        return index.get(term);
    }

    /**
     * @param term: a string describing a word
     *      creates the posting list of the given term
     * **/
    private static ArrayList<MutablePair<Integer, Integer>> createPostingList(String term){
        ArrayList<MutablePair<Integer, Integer> >postingList = new ArrayList<>(); //create posting list
        index.put(term,postingList); //add entry to index
        return postingList;
    }


    public static void spimi(){

        FileUtils.createOrCleanFile(PATH_TO_DOCUMENT_INDEX); //create the file containing document index

        try(BufferedReader br = Files.newBufferedReader(Paths.get(PATH_TO_DOCUMENTS))){

            int pid; //docno of document
            String[] terms; //terms contained in the document
            String[] document; //document[0] -> pid  document[1]-> terms
            String line; //line read from disk
            ArrayList<MutablePair<Integer,Integer>> posting; //posting list of a given term
            boolean allDocumentsProcessed = false;

            int docid = 0; //assign docid in a incremental manner

            while(!allDocumentsProcessed) {

                while (Runtime.getRuntime().freeMemory() > MEMORY_TRESHOLD ) { //build index until memory is available
                                                                                //taking into account a threshold

                    System.out.println(Runtime.getRuntime().freeMemory());

                    line = br.readLine();
                    if(line == null){ // all documents were processed
                        allDocumentsProcessed = true;
                        break;
                    }
                    //parse line to get pid and all terms of a document
                    document = line.split("\t");
                    pid = Integer.parseInt(document[0]);
                    if(document.length > 1) { //TODO: preprocessing returns empty documents!!
                        terms = document[1].split(",");

                        //create new document index entry and save it onto disk in format docid \t pid,document length \n
                        String entry = docid++ + "\t" + pid + "," + terms.length + "\n";
                        writeEntry(PATH_TO_DOCUMENT_INDEX, entry);

                        for (String term : terms) {

                            if (!index.containsKey(term)) {
                                // create new posting list if term wasn't present yet
                                posting = createPostingList(term);
                            } else {
                                //term is present, we can get its posting list
                                posting = getPostingList(term);
                            }

                            updateOrAddPosting(docid, posting); //insert or update new posting
                        }
                    }
                }

                //either if there is no  memory available or all documents were read, flush partial index onto disk
                save_index_to_disk(PATH_BASE_TO_INDEX+num_index+".txt");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
