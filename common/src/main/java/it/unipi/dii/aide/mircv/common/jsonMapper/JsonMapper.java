package it.unipi.dii.aide.mircv.common.jsonMapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.unipi.dii.aide.mircv.common.dto.ProcessedCollectionDTO;
import it.unipi.dii.aide.mircv.common.dto.ProcessedDocumentDTO;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class JsonMapper {
    public static void toJson(ArrayList<ProcessedDocumentDTO> documents, String outputPath) {
        ObjectMapper mapper = new ObjectMapper();
        ProcessedCollectionDTO collection = new ProcessedCollectionDTO(documents);

        try {
            mapper.writeValue(new File(outputPath), collection);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
