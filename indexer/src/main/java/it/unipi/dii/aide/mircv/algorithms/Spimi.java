package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.beans.PostingList;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.dto.ProcessedDocumentDTO;
import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;
import it.unipi.dii.aide.mircv.utils.Utility;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;


public class Spimi {

    //TODO: javadocs [Benedetta]

    //path to the file on the disk storing the processed collection
    private static final String PATH_TO_DOCUMENTS = ConfigurationParameters.getProcessedCollectionPath();

    //path to the file on the disk storing the partial indexes
    private static final String PATH_BASE_TO_INDEX = ConfigurationParameters.getPartialIndexPath();

    //path to the file on the disk storing the document index
    private static final String PATH_TO_DOCUMENT_INDEX = ConfigurationParameters.getDocumentIndexPath();

    //chunk of memory to be kept free
//    private static final long MEMORY_TRESHOLD = 1048576; //1MB
    private static final long MEMORY_TRESHOLD = 80000000;

    //structure storing the partial inverted index
    //private static HashMap<String, ArrayList<MutablePair<Integer, Integer>>> index = new HashMap<>();

    //counts the number of partial indexes created
    private static int num_index = 0;



    private static void saveIndexToDisk(HashMap<String, PostingList> index){
        if(index.isEmpty()) //if the index is empty there is nothing to write on disk
            return;

        //sort index in lexicographic order
        index = index.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));


        try(DB db = DBMaker.fileDB(PATH_BASE_TO_INDEX + num_index + ".db").fileChannelEnable().fileMmapEnable().make()){
            ArrayList<PostingList> partialIndex = (ArrayList<PostingList>) db.indexTreeList("index_" + num_index, Serializer.JAVA).createOrOpen();
            partialIndex.addAll(index.values());

            //update number of partial inverted indexes
            num_index ++;
        }catch(Exception e){
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
    private static void updateOrAddPosting(int docid, PostingList postingList){
        boolean found = false;
        for(Map.Entry<Integer, Integer> posting: postingList.getPostings()){ //iterate for each posting in postinglist

            if(docid == posting.getKey()){ //docid found
                posting.setValue(posting.getValue() + 1); //update frequency
                found = true;
            }
        }

        if(!found) //document with that docid wasn't int the posting list
            postingList.getPostings().add(new AbstractMap.SimpleEntry<>(docid,1)); //create new pair and add it

    }

    public static void spimi(){

        try(DB db = DBMaker.fileDB(PATH_TO_DOCUMENTS).fileChannelEnable().fileMmapEnable().make();
            DB docIndexDb = DBMaker.fileDB(PATH_TO_DOCUMENT_INDEX).fileChannelEnable().fileMmapEnable().make();
            HTreeMap collection = db.hashMap("processedCollection")
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(Serializer.JAVA)
                    .createOrOpen();
            HTreeMap<Integer, String> docIndex= docIndexDb.hashMap("docIndex")
                    .keySerializer(Serializer.INTEGER)
                    .valueSerializer(Serializer.STRING)
                    .createOrOpen()
        ){
            boolean allDocumentsProcessed = false;

            int docid = 0; //assign docid in a incremental manner

            while(!allDocumentsProcessed) {
                HashMap<String, PostingList> index = new HashMap<>();;
                while (Runtime.getRuntime().freeMemory() > MEMORY_TRESHOLD ) { //build index until memory is available
                    // taking into account a threshold


//                    System.out.println(Runtime.getRuntime().freeMemory());

                    Iterator<String> keyterator = (Iterator<String>) collection.keySet().iterator();
                    if(!keyterator.hasNext()){ // all documents were processed
                        allDocumentsProcessed = true;
                        break;
                    }
                    //parse line to get pid and all terms of a document
                    ProcessedDocumentDTO document = new ProcessedDocumentDTO();
                    String pid = keyterator.next();
                    document.setPid(pid);
                    document.setTokens((ArrayList<String>) collection.get(pid));

                    //create new document index entry and save it onto disk in format docid \t pid,document length \n
                    String entry = docid++ + "\t" + pid + "," + document.getTokens().size() + "\n";
                    docIndex
                    System.out.println(docid);
                    CollectionStatistics.addDocument();
                    for (String term : document.getTokens()) {
                        PostingList posting;
                        if (!index.containsKey(term)) {
                           // create new posting list if term wasn't present yet
                            posting = new PostingList(term);
                            index.put(term, posting);
                        } else {
                            //term is present, we can get its posting list
                            posting = index.get(term);
                        }

                        updateOrAddPosting(docid, posting); //insert or update new posting
                    }
                    saveIndexToDisk(index);
                }

                //either if there is no  memory available or all documents were read, flush partial index onto disk

            }
            Utility.setNumIndexes(num_index);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
