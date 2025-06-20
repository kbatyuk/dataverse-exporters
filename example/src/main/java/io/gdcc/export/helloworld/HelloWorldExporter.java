package io.gdcc.export.helloworld;

import com.google.auto.service.AutoService;
import io.gdcc.spi.export.ExportDataProvider;
import io.gdcc.spi.export.ExportException;
import io.gdcc.spi.export.Exporter;
import java.io.OutputStream;
import java.util.Locale;

import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.ws.rs.core.MediaType;

@AutoService(Exporter.class)
public class HelloWorldExporter implements Exporter {

    @Override
    public String getFormatName() {
        return "dataverse_json";
    }

    @Override
    public String getDisplayName(Locale locale) {
        return "My JSON in " + locale.getLanguage();
    }

    @Override
    public Boolean isHarvestable() {
        return false;
    }

    @Override
    public Boolean isAvailableToUsers() {
        return true;
    }

    @Override
    public String getMediaType() {
        return MediaType.APPLICATION_JSON;
    }

    @Override
    public void exportDataset(ExportDataProvider dataProvider, OutputStream outputStream) throws ExportException {
        try {
            JsonObject inputJson = dataProvider.getDatasetJson().asJsonObject();

            JsonObject datasetVersion = inputJson.getJsonObject("datasetVersion");
            JsonObject metadataBlocks = datasetVersion.getJsonObject("metadataBlocks");

            // Cruise fields
            JsonObject cruiseBlock = metadataBlocks.getJsonObject("cruise");
            JsonObject awardsBlock = metadataBlocks.getJsonObject("awards");
            JsonObject personsBlock = metadataBlocks.getJsonObject("persons");
            JsonObject scheduleBlock = metadataBlocks.getJsonObject("scheduler_records");

            JsonObjectBuilder cruiseBuilder = Json.createObjectBuilder();

            // 1. Cruise id (from cnumber field array)
            String cruiseId = getFieldValueFromArray(cruiseBlock, "cnumber");
            cruiseBuilder.add("id", cruiseId);

            // 2. schedule_records
            JsonArrayBuilder scheduleRecordsBuilder = Json.createArrayBuilder();
            if (scheduleBlock != null && scheduleBlock.containsKey("fields")) {
                for (var field : scheduleBlock.getJsonArray("fields")) {
                    JsonObject record = (JsonObject) field;
                    if ("scheduler_id".equals(record.getString("typeName"))) {
                        String schedulerId = record.getString("value", "");
                        String schedulerCruiseId = getFieldValue(scheduleBlock, "scheduler_id");
                        scheduleRecordsBuilder.add(Json.createObjectBuilder()
                                .add("scheduler_id", getFieldValue(scheduleBlock, "scheduler"))
                                .add("scheduler_cruise_id", schedulerCruiseId)
                        );
                        break;
                    }
                }
            }
            cruiseBuilder.add("schedule_records", scheduleRecordsBuilder);

            // 3. name
            String cruiseName = getFieldValue(datasetVersion.getJsonObject("metadataBlocks").getJsonObject("citation"), "title");
            cruiseBuilder.add("name", cruiseName);

            // 4. vessel_id
            cruiseBuilder.add("vessel_id", getFieldValue(cruiseBlock, "vessel_id"));

            // 5. depart_date
            cruiseBuilder.add("depart_date", getFieldValue(cruiseBlock, "depart_date"));

            // 6. depart_port_id
            cruiseBuilder.add("depart_port_id", getFieldValue(cruiseBlock, "depart_port_id"));

            // 7. arrive_date
            cruiseBuilder.add("arrive_date", getFieldValue(cruiseBlock, "arrive_date"));

            // 8. arrive_port_id
            // Note the typo in sample JSON: "arrive_poer_id"
            cruiseBuilder.add("arrive_port_id", getFieldValue(cruiseBlock, "arrive_poer_id"));

            // 9. persons
            JsonArrayBuilder personsBuilder = Json.createArrayBuilder();
            if (personsBlock != null && personsBlock.containsKey("fields")) {
                for (var field : personsBlock.getJsonArray("fields")) {
                    JsonObject personField = (JsonObject) field;
                    if ("person_n".equals(personField.getString("typeName"))) {
                        for (var personVal : personField.getJsonArray("value")) {
                            JsonObject personObj = (JsonObject) personVal;
                            personsBuilder.add(Json.createObjectBuilder()
                                .add("id", personObj.getString("person_name", ""))
                                .add("name", personObj.getString("person_name", ""))
                                .add("institution_id", personObj.getString("instituion_id", ""))
                                .add("role", personObj.getString("role", ""))
                            );
                        }
                    }
                }
            }
            cruiseBuilder.add("persons", personsBuilder);

            // 10. awards
            JsonArrayBuilder awardsBuilder = Json.createArrayBuilder();
            if (awardsBlock != null && awardsBlock.containsKey("fields")) {
                JsonObject awardFields = awardsBlock.getJsonArray("fields").getJsonObject(0);
                awardsBuilder.add(Json.createObjectBuilder()
                        .add("project_number", awardFields.getString("project_number", ""))
                        .add("name", awardFields.getString("project_name", ""))
                        .add("agency_id", awardFields.getString("agency_id", ""))
                        .add("agency_department", awardFields.getString("agency_department", ""))
                );
            }
            cruiseBuilder.add("awards", awardsBuilder);

            // 11. embargoes (from termsOfAccess)
            String embargoes = datasetVersion.getString("termsOfAccess", "none");
            cruiseBuilder.add("embargoes", embargoes);

            // Build the top-level object
            JsonObject output = Json.createObjectBuilder()
                .add("cruise", cruiseBuilder)
                .build();

            outputStream.write(output.toString().getBytes("UTF8"));
            outputStream.flush();
        } catch (Exception e) {
            throw new ExportException("Unknown exception caught during JSON export: " + e.getMessage());
        }
    }

    // Helper function to get a primitive field value from a metadata block
    private String getFieldValue(JsonObject block, String typeName) {
        if (block == null || !block.containsKey("fields")) return "";
        for (var field : block.getJsonArray("fields")) {
            JsonObject f = (JsonObject) field;
            if (typeName.equals(f.getString("typeName"))) {
                return f.getString("value", "");
            }
        }
        return "";
    }

    // Helper to get the first value from a primitive array field
    private String getFieldValueFromArray(JsonObject block, String typeName) {
        if (block == null || !block.containsKey("fields")) return "";
        for (var field : block.getJsonArray("fields")) {
            JsonObject f = (JsonObject) field;
            if (typeName.equals(f.getString("typeName")) && f.get("value") != null && f.get("value").getValueType() == jakarta.json.JsonValue.ValueType.ARRAY) {
                return f.getJsonArray("value").getString(0, "");
            }
        }
        return "";
    }
}
