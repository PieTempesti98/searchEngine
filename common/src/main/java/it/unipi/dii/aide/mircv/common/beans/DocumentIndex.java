package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.HashMap;
import java.util.Map;

public class DocumentIndex extends HashMap<Integer, DocumentIndexEntry> {

    private static DocumentIndex instance = null;

    private DocumentIndex(){
        //use DBMaker to create a DB object of HashMap stored on disk
        //provide location
        DB db = DBMaker.fileDB(ConfigurationParameters.getDocumentIndexPath()).fileMmapEnable().fileChannelEnable().make();

        Map<Integer, DocumentIndexEntry> docIndex = (Map<Integer, DocumentIndexEntry>) db.hashMap("docIndex")
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(Serializer.JAVA).createOrOpen();

        //read from map
        for(int docid: docIndex.keySet()){
            this.put(docid, docIndex.get(docid));
        }

        //close to protect from data corruption
        db.close();
    }

    public static DocumentIndex getInstance(){
        if(instance == null){
            instance = new DocumentIndex();
        }
        return instance;
    }

    public String getPid(int docid){
        return this.get(docid).getPid();
    }

}