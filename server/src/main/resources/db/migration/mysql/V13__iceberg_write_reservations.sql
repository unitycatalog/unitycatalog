CREATE TABLE uc_iceberg_write_reservations (
    id binary(16) NOT NULL,
    name varchar(255) NOT NULL,
    storage_location varchar(4096) NOT NULL,
    PRIMARY KEY (id)
) engine=InnoDB;
