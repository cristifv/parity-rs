CREATE TABLE tenant_route (
    tenant_id       text        PRIMARY KEY,
    home_zone       text        NOT NULL,
    zone            text        NOT NULL,
    cell            text        NOT NULL,
    deployment_unit text        NOT NULL,
    version         bigint      NOT NULL,
    updated_at      timestamptz NOT NULL
);
