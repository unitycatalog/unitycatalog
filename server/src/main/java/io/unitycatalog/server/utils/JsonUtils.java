package io.unitycatalog.server.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;

public class JsonUtils {

  @Getter private static final ObjectMapper instance = new ObjectMapper();
}
