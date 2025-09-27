package io.unitycatalog.spark;

import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import org.apache.hadoop.shaded.com.google.gson.JsonObject;
import org.apache.hadoop.shaded.com.google.gson.JsonParser;

public interface TempCredentialRequest {
    TempCredRequestType type();

    String serialize();

    static TempCredentialRequest deserialize(String content) {
        JsonObject json = JsonParser.parseString(content).getAsJsonObject();
        String type = json.getAsJsonPrimitive("type").getAsString();
        switch (TempCredRequestType.of(type)){
            case PATH -> {
                String path = json.getAsJsonPrimitive("path").getAsString();
                String operation = json.getAsJsonPrimitive("operation").getAsString();
                return new TempPathCredentialRequest(path, PathOperation.fromValue(operation));
            }
            case TABLE -> {
                String tableId = json.getAsJsonPrimitive("tableId").getAsString();
                String operation = json.getAsJsonPrimitive("operation").getAsString();
                return new TempTableCredentialRequest(tableId, TableOperation.fromValue(operation));
            }
        }

        throw  new UnsupportedOperationException();
    }

    enum TempCredRequestType {
        PATH("PATH"),
        TABLE("TABLE");

        private String type;

        TempCredRequestType(String type) {
            this.type = type;
        }

        @Override
        public String toString(){
            return type;
        }

        public static TempCredRequestType of(String value){
            for (TempCredRequestType t : TempCredRequestType.values()) {
                if (t.type.equals(value)) {
                    return t;
                }
            }
            throw new IllegalArgumentException("Unexpected value: " + value);
        }
    }

    class TempPathCredentialRequest implements TempCredentialRequest {
        private final String path;
        private final PathOperation operation;

        private TempPathCredentialRequest(String path, PathOperation operation) {
            this.path = path;
            this.operation = operation;
        }

        public static TempPathCredentialRequest of(String path, PathOperation operation) {
            return new TempPathCredentialRequest(path, operation);
        }

        public String path(){
            return path;
        }

        public PathOperation operation(){
            return operation;
        }

        @Override
        public TempCredRequestType type() {
            return TempCredRequestType.PATH;
        }

        @Override
        public String serialize() {
            JsonObject json = new JsonObject();
            json.addProperty("type", TempCredRequestType.PATH.toString());
            json.addProperty("path", path);
            json.addProperty("operation", operation.name());
            return json.toString();
        }
    }

    class TempTableCredentialRequest implements TempCredentialRequest {
        private final String tableId;
        private final TableOperation operation;

        private TempTableCredentialRequest(String tableId, TableOperation operation) {
            this.tableId = tableId;
            this.operation = operation;
        }

        public static TempTableCredentialRequest of(String tableId, TableOperation operation) {
            return new TempTableCredentialRequest(tableId, operation);
        }

        public String tableId(){
            return tableId;
        }

        public TableOperation operation(){
            return operation;
        }

        @Override
        public TempCredRequestType type() {
            return TempCredRequestType.TABLE;
        }

        @Override
        public String serialize() {
            JsonObject json = new JsonObject();
            json.addProperty("type", TempCredRequestType.TABLE.toString());
            json.addProperty("tableId", tableId);
            json.addProperty("operation", operation.name());
            return json.toString();
        }
    }
}
