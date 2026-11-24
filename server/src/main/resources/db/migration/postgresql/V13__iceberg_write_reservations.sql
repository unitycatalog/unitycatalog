CREATE TABLE uc_iceberg_write_reservations (
    id uuid NOT NULL,
    name varchar(255) NOT NULL,
    storage_location varchar(4096) NOT NULL,
    PRIMARY KEY (id)
);
