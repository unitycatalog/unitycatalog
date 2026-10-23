package unitycatalog

import rego.v1

default allow := false

# A malformed decision exercises the real UC error path without replacing the OPA server.
decision := "true" if {
    input.principal.email == "invalid@example.com"
} else := allow

allow if {
    input.principal.email == "admin"
    is_string(input.principal.id)
}

allow if {
    input.principal.email == "writer@example.com"
    input.action == "CREATE_CATALOG"
    input.context.name == "visible"
    is_string(input.resources.metastore)
}

allow if {
    input.principal.email == "writer@example.com"
    input.action == "CREATE_USER"
    input.context.email == "created@example.com"
    is_string(input.resources.metastore)
}

allow if {
    input.principal.email == "writer@example.com"
    input.action == "RENAME_TABLE"
    input.resources.table == data.test.table
    input.context.new_name == "renamed"
}

allow if {
    input.principal.email == "reader@example.com"
    input.action in {"GET_CATALOG", "LIST_CATALOGS"}
    input.resource_keys.catalog == "visible"
    is_string(input.resources.catalog)
}

allow if {
    input.principal.email == "reader@example.com"
    input.action == "GENERATE_PATH_CREDENTIALS"
    input.context.operation == "PATH_READ"
    input.resources.external_location == data.test.external_location
    input.resource_keys.external_location == "s3://test-bucket/data/free"
    input.context.no_overlap_with_data_securable
}

allow if {
    input.principal.email == "reader@example.com"
    input.action == "GENERATE_PATH_CREDENTIALS"
    input.context.operation == "PATH_READ"
    some kind in {"table", "volume", "registered_model"}
    input.resources[kind] == data.test[kind]
    input.resources.catalog == data.test.catalog
    input.resources.schema == data.test.schema
    not input.resources.external_location
    not input.context.no_overlap_with_data_securable
}

allow if {
    input.principal.email in {"reader@example.com", "writer@example.com", "write_only@example.com"}
    input.action == "GET_TABLE"
    input.resources.table == data.test.table
}

allow if {
    input.principal.email in {"reader@example.com", "writer@example.com"}
    input.action == "GENERATE_TABLE_CREDENTIALS"
    input.resources.table == data.test.table
    input.context.operation == "READ"
}

allow if {
    input.principal.email in {"writer@example.com", "write_only@example.com"}
    input.action == "GENERATE_TABLE_CREDENTIALS"
    input.resources.table == data.test.table
    input.context.operation == "READ_WRITE"
}
