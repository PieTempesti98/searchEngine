package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.HashMap;
import java.util.Map;

public class Vocabulary extends HashMap<String, VocabularyEntry>{

    private static Vocabulary instance = null;

    private Vocabulary(){
        //use DBMaker to create a DB object of HashMap stored on disk
        //provide location
        DB db = DBMaker.fileDB(ConfigurationParameters.getVocabularyPath()).make();

        //use the DB object to open the "myMap" HashMap
        Map<String, VocabularyEntry> vocabulary = (Map<String, VocabularyEntry>) db.hashMap("vocabulary")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA).createOrOpen();

        System.out.println(vocabulary.size());
        //read from map
        for(String term: vocabulary.keySet()){
            this.put(term, vocabulary.get(term));
            System.out.println(vocabulary.get(term));
        }

        //close to protect from data corruption
        db.close();
    }

    public static Vocabulary getInstance(){
        if(instance == null){
            instance = new Vocabulary();
        }
        return instance;
    }

    public double getIdf(String term){
        return this.get(term).getIdf();
    }


}
