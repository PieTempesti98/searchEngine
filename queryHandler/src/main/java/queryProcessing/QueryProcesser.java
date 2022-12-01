package queryProcessing;

import it.unipi.dii.aide.mircv.beans.DocumentIndexEntry;
import it.unipi.dii.aide.mircv.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;
import it.unipi.dii.aide.mircv.common.beans.TextDocument;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class QueryProcesser {

    /**
     * path to file storing inverted index
     */
    private static final String PATH_TO_INVERTED_INDEX = ConfigurationParameters.getInvertedIndexPath();

    /**
     * checks if the data structures needed for query processing were correctly created
     * @return boolean
     */
    public static boolean setupProcesser(){

        //check if document index exists. If not the setup failed
        if(! new File(PATH_TO_INVERTED_INDEX).exists())
            return false;

        //check if vocabulary and document index were correctly created. If not the setup failed
        if(vocabulary.isEmpty() || documentIndex.isEmpty())
            return false;

        //successful setup
        return true;
    }


}
