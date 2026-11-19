    -- The owner indexes go last: on MySQL a failed step cannot roll back, so a rerun after a
    -- repair has to work, and the table the index needs is of an earlier step. MySQL has no "if
    -- not exists" for indexes.

    create index idx_shares_owner_id on os_shares (owner_id);
