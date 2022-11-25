package it.unipi.dii.aide.mircv.common.jsonMapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.unipi.dii.aide.mircv.common.beans.ProcessedCollection;
import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class JsonMapper {
    public static void toJson(ArrayList<ProcessedDocument> documents, String outputPath) {
        ObjectMapper mapper = new ObjectMapper();
        ProcessedCollection collection = new ProcessedCollection(documents);

        try {
            mapper.writeValue(new File(outputPath), collection);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
