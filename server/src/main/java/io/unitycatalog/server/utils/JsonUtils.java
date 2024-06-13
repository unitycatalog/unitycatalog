package io.unitycatalog.server.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;

public class JsonUtils {

    @Getter
    private static final ObjectMapper instance = new ObjectMapper();

    private static final int MAX_COLUMN_WIDTH = 15;

    public static void printJsonAsTable(String jsonString) {
        try {
            JsonNode rootNode = instance.readTree(jsonString);
            // Determine the maximum width for each column
            Map<String, Integer> columnWidths = new HashMap<>();
            if (rootNode.isArray()) {
                for (JsonNode element : rootNode) {
                    updateColumnWidths(columnWidths, element);
                }
            } else {
                updateColumnWidths(columnWidths, rootNode);
            }

            // Print the table with adjusted column widths
            if (rootNode.isArray()) {
                Iterator<JsonNode> elements = rootNode.elements();
                if (elements.hasNext()) {
                    JsonNode firstElement = elements.next();
                    printHeaders(firstElement, columnWidths);
                    printRow(firstElement, columnWidths);

                    while (elements.hasNext()) {
                        JsonNode element = elements.next();
                        printRow(element, columnWidths);
                    }
                }
            } else {
                printHeaders(rootNode, columnWidths);
                printRow(rootNode, columnWidths);
            }

        } catch (JsonMappingException e) {
            System.out.println(jsonString);
        } catch (JsonProcessingException e) {
            System.out.println(jsonString);
        }
    }

    private static void updateColumnWidths(Map<String, Integer> columnWidths, JsonNode node) {
        node.fields().forEachRemaining(field -> {
            String key = field.getKey();
            JsonNode value = field.getValue();
            int valueLength = value.isArray() || value.isObject() ? value.toString().length() : value.asText().length();
            columnWidths.put(key, Math.min(Math.max(columnWidths.getOrDefault(key, key.length()), valueLength), MAX_COLUMN_WIDTH));
        });
    }

    private static void printHeaders(JsonNode node, Map<String, Integer> columnWidths) {
        System.out.print("|");
        node.fieldNames().forEachRemaining(field -> System.out.printf(" %-"+columnWidths.get(field)+"s |", truncate(field, columnWidths.get(field))));
        System.out.println();
        System.out.print("|");
        node.fieldNames().forEachRemaining(field -> System.out.print("-".repeat(columnWidths.get(field) + 2) + "|"));
        System.out.println();
    }

    private static void printRow(JsonNode node, Map<String, Integer> columnWidths) {
        System.out.print("|");
        node.fields().forEachRemaining(field -> {
            String value = field.getValue().isArray() || field.getValue().isObject() ? field.getValue().toString() : field.getValue().asText();
            System.out.printf(" %-"+columnWidths.get(field.getKey())+"s |", truncate(value, columnWidths.get(field.getKey())));
        });
        System.out.println();
    }

    private static String truncate(String value, int maxWidth) {
        if (value.length() > maxWidth) {
            return value.substring(0, maxWidth - 3) + "..."; // Truncate and add ellipsis
        }
        return value;
    }
}
