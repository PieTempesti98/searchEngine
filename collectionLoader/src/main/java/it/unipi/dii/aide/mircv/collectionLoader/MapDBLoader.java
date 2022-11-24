package it.unipi.dii.aide.mircv.collectionLoader;

import it.unipi.dii.aide.mircv.collectionLoader.beans.TextDocument;
import it.unipi.dii.aide.mircv.collectionLoader.preprocess.Preprocesser;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.dto.ProcessedDocumentDTO;
import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import static it.unipi.dii.aide.mircv.common.utils.FileUtils.removeFile;

public class MapDBLoader {
    /**
     * Path to the raw collection
     */

    private static final String PATH_TO_COLLECTION = ConfigurationParameters.getRawCollectionPath();

    /**
     * Path to document in which we can save the whole collection
     */
    private static final String OUTPUT_PATH = ConfigurationParameters.getProcessedCollectionPath();

    /**
     * Main method for the preprocessing module:
     * <ol>
     *     <li>Parses the document from the raw file one at a time</li>
     *     <li>Processes the document, transforming it in a list of tokens</li>
     *     <li>save it to a new file</li>
     * </ol>
     * @param args input parameters for the main method, not used
     */
    public static void main(String[] args) {
        // load the stopwords into the preprocesser
        Preprocesser.readStopwords();

        removeFile(OUTPUT_PATH);

        try(BufferedReader br = Files.newBufferedReader(Paths.get(PATH_TO_COLLECTION), StandardCharsets.UTF_8);
            DB db = DBMaker.fileDB(OUTPUT_PATH).fileChannelEnable().fileMmapEnable().make();
            HTreeMap<String, ArrayList<String>> processedCollection = db.hashMap("processedCollection")
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(Serializer.JAVA)
                    .createOrOpen();){
            String[] split;

            for(String line; (line = br.readLine()) != null; ){

                // if the line is empty we process the next line
                if(line.isEmpty())
                    continue;

                // split of the line in the format <pid>\t<text>
                split = line.split("\t");

                // Creation of the text document for the line
                TextDocument document = new TextDocument(split[0], split[1].replaceAll("[^\\x00-\\x7F]", ""));

                // Perform text preprocessing on the document
                ProcessedDocumentDTO processedDocument = Preprocesser.processDocument(document);

                if(processedDocument.getTokens().size() > 0)
                    //Save it to the file if body is non empty
                    processedCollection.put(processedDocument.getPid(), processedDocument.getTokens());
                // Update the number of documents
                CollectionStatistics.addDocument();
            }

        } catch(Exception e){
            e.printStackTrace();
        }

    }

}
