    -- os_share_properties references os_shares, of the step before.

    create table if not exists os_share_properties (
        share_id uuid not null,
        property_value varchar(1000),
        property_key varchar(255) not null,
        constraint uk_share_properties primary key (share_id, property_key),
        constraint fk_share_properties_share foreign key (share_id) references os_shares (id)
    );
