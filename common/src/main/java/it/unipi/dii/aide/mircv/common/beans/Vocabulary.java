package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import java.util.HashMap;

public class Vocabulary extends HashMap<String, VocabularyEntry>{

    private static Vocabulary instance = null;
    private final String VOCABULARY_PATH = ConfigurationParameters.getVocabularyPath();

    public static Vocabulary getInstance(){
        if(instance == null){
            instance = new Vocabulary();
        }
        return instance;
    }

    public double getIdf(String term){
        return this.get(term).getIdf();
    }

    public boolean writeToDisk() { //TODO: is useful?


        long position = 0;

        for (VocabularyEntry entry : this.values()) {

            position = entry.writeEntryToDisk(0,this.VOCABULARY_PATH);
            if(position == -1)
                return false;

        }

        return true;


    }

    public boolean readFromDisk(){

        long position = 0;

        //read whole vocabulary from
        while(position >= 0){
            VocabularyEntry entry = new VocabularyEntry();

            //read entry and update position
            position = entry.readFromDisk(position,this.VOCABULARY_PATH);

            //populate vocabulary
            this.put(entry.getTerm(),entry);
        }

        //if position == -1 an error occurred during reading
        return position != -1;

    }

    /**
    *
    * @param term: term of which we want vocabulary entry
    * @return the vocabulary entry of given term
    **/
    public VocabularyEntry findEntry(String term){

        VocabularyEntry entry = new VocabularyEntry();

        //performs binary search for input term
        int start = 0;
        int end = 1000; //TODO: update parameter with the final collection size
        int mid;
        String key;

        long entrySize = entry.getENTRY_SIZE();

        while (start < end) {

            mid = (start + end) / 2;

            entry.readFromDisk(mid * entrySize, this.VOCABULARY_PATH);
            key = entry.getTerm();

            if (key.equals(term))
                return entry;


            if (term.compareTo(key) > 0) {

                start = mid + 1;
                continue;
            }

            end = mid - 1;
        }
        return null;
    }



}
