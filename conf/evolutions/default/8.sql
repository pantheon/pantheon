# --- !Ups
--remove 'valid' field from schema
ALTER TABLE schemas DROP COLUMN valid;

--this extension provides 'uuid_generate_v4()' function
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

--------Unlinking unused tables
ALTER TABLE tbls DROP CONSTRAINT tbls_data_source_id_fkey;

--changing id of saved_queries
ALTER TABLE saved_queries DROP COLUMN saved_query_id;
ALTER TABLE saved_queries ADD COLUMN saved_query_id uuid;
UPDATE saved_queries SET saved_query_id=uuid_generate_v4();
ALTER TABLE saved_queries ADD CONSTRAINT saved_queries_pkey PRIMARY KEY(saved_query_id);

--------Relinking catalogs

--adding uuid columns(they will replace current pks and fks)
ALTER TABLE catalogs ADD COLUMN newpk uuid;
ALTER TABLE schemas ADD COLUMN newfk uuid;
ALTER TABLE backend_configs ADD COLUMN newfk uuid;
ALTER TABLE data_sources ADD COLUMN newfk uuid;
ALTER TABLE saved_queries ADD COLUMN newfk uuid;
ALTER TABLE query_history ADD COLUMN newfk uuid;

--initializing new pk of parent with random value and linking child tables
UPDATE catalogs SET newpk  = uuid_generate_v4();
UPDATE schemas s SET newfk = (select newpk from catalogs c where c.catalog_id = s.catalog_id);
UPDATE backend_configs s SET newfk = (select newpk from catalogs c where c.catalog_id = s.catalog_id);
UPDATE data_sources s SET newfk = (select newpk from catalogs c where c.catalog_id = s.catalog_id);
UPDATE saved_queries s SET newfk = (select newpk from catalogs c where c.catalog_id = s.catalog_id);
UPDATE query_history s SET newfk = (select newpk from catalogs c where c.catalog_id = s.catalog_id);

--dropping old fk and renaming new fk in child tables
ALTER TABLE schemas DROP COLUMN catalog_id;
ALTER TABLE schemas RENAME COLUMN newfk to catalog_id;
ALTER TABLE backend_configs DROP COLUMN catalog_id;
ALTER TABLE backend_configs RENAME COLUMN newfk to catalog_id;
ALTER TABLE data_sources DROP COLUMN catalog_id;
ALTER TABLE data_sources RENAME COLUMN newfk to catalog_id;
ALTER TABLE saved_queries DROP COLUMN catalog_id;
ALTER TABLE saved_queries RENAME COLUMN newfk to catalog_id;
ALTER TABLE query_history DROP COLUMN catalog_id;
ALTER TABLE query_history RENAME COLUMN newfk to catalog_id;

--dropping old pk, renaming new pk  and adding pk constraint in parent table
ALTER TABLE catalogs DROP COLUMN catalog_id;
ALTER TABLE catalogs RENAME COLUMN newpk to catalog_id;
ALTER TABLE catalogs ADD CONSTRAINT catalogs_pkey PRIMARY KEY(catalog_id);

--add fk and not null constraints to all child tables
ALTER TABLE schemas ADD CONSTRAINT schemas_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES catalogs(catalog_id);
ALTER TABLE schemas ALTER COLUMN catalog_id SET NOT NULL;
ALTER TABLE backend_configs ADD CONSTRAINT backend_configs_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES catalogs(catalog_id);
ALTER TABLE backend_configs ALTER COLUMN catalog_id SET NOT NULL;
ALTER TABLE data_sources ADD CONSTRAINT data_sources_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES catalogs(catalog_id);
ALTER TABLE data_sources ALTER COLUMN catalog_id SET NOT NULL;
ALTER TABLE saved_queries ADD CONSTRAINT saved_queries_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES catalogs(catalog_id);
ALTER TABLE saved_queries ALTER COLUMN catalog_id SET NOT NULL;
ALTER TABLE query_history ADD CONSTRAINT query_history_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES catalogs(catalog_id);
ALTER TABLE query_history ALTER COLUMN catalog_id SET NOT NULL;


-------Relinking backend_configs

--adding uuid columns(they will replace current pks and fks)
ALTER TABLE backend_configs ADD COLUMN newpk uuid;
ALTER TABLE schemas ADD COLUMN newfk uuid;
ALTER TABLE catalogs ADD COLUMN newfk uuid;

--initializing new pk of parent with random value and linking child tables
UPDATE backend_configs SET newpk = uuid_generate_v4();
UPDATE schemas s SET newfk = (select newpk from backend_configs c where c.backend_config_id = s.backend_config_id);
UPDATE catalogs s SET newfk = (select newpk from backend_configs c where c.backend_config_id = s.backend_config_id);

--dropping old fk and renaming new fk in child tables
ALTER TABLE schemas DROP COLUMN backend_config_id;
ALTER TABLE schemas RENAME COLUMN newfk to backend_config_id;
ALTER TABLE catalogs DROP COLUMN backend_config_id;
ALTER TABLE catalogs RENAME COLUMN newfk to backend_config_id;

--dropping old pk, renaming new pk  and adding pk constraint in parent table
ALTER TABLE backend_configs DROP COLUMN backend_config_id;
ALTER TABLE backend_configs RENAME COLUMN newpk to backend_config_id;
ALTER TABLE backend_configs ADD CONSTRAINT backend_configs_pkey PRIMARY KEY(backend_config_id);

--add fk and not null constraints to all child tables
ALTER TABLE schemas ADD CONSTRAINT schemas_backend_config_id_fkey FOREIGN KEY (backend_config_id) REFERENCES backend_configs(backend_config_id);
ALTER TABLE catalogs ADD CONSTRAINT catalogs_backend_config_id_fkey FOREIGN KEY (backend_config_id) REFERENCES backend_configs(backend_config_id);

---------Relinking jdbc_products

--adding uuid columns(they will replace current pks and fks)
ALTER TABLE jdbc_products ADD COLUMN newpk uuid;
ALTER TABLE data_sources ADD COLUMN newfk uuid;

--initializing new pk of parent with random value and linking child tables
UPDATE jdbc_products SET newpk = uuid_generate_v4();
UPDATE data_sources s SET newfk = (select newpk from jdbc_products c where c.jdbc_product_id = s.jdbc_product_id);

--dropping old fk and renaming new fk in child tables
ALTER TABLE data_sources DROP COLUMN jdbc_product_id;
ALTER TABLE data_sources RENAME COLUMN newfk to jdbc_product_id;

--dropping old pk, renaming new pk  and adding pk constraint in parent table
ALTER TABLE jdbc_products DROP COLUMN jdbc_product_id;
ALTER TABLE jdbc_products RENAME COLUMN newpk to jdbc_product_id;
ALTER TABLE jdbc_products ADD CONSTRAINT jdbc_product_pkey PRIMARY KEY(jdbc_product_id);

--add fk constraints to all child tables
ALTER TABLE data_sources ADD CONSTRAINT data_sources_jdbc_product_id_fkey FOREIGN KEY (jdbc_product_id) REFERENCES jdbc_products(jdbc_product_id);


---------Relinking schemas

--adding uuid columns(they will replace current pks and fks)
ALTER TABLE schemas ADD COLUMN newpk uuid;
ALTER TABLE data_source_links ADD COLUMN newfk uuid;
ALTER TABLE schema_usages ADD COLUMN newfkp uuid;
ALTER TABLE schema_usages ADD COLUMN newfkc uuid;

--initializing new pk of parent with random value and linking child tables
UPDATE schemas SET newpk = uuid_generate_v4();
UPDATE data_source_links s SET newfk = (select newpk from schemas c where c.schema_id = s.schema_id);
UPDATE schema_usages s SET newfkp = (select newpk from schemas c where c.schema_id = s.parent_id);
UPDATE schema_usages s SET newfkc = (select newpk from schemas c where c.schema_id = s.child_id);

--dropping old fk and renaming new fk in child tables
ALTER TABLE data_source_links DROP COLUMN schema_id;
ALTER TABLE data_source_links RENAME COLUMN newfk to schema_id;
ALTER TABLE schema_usages DROP COLUMN parent_id;
ALTER TABLE schema_usages RENAME COLUMN newfkp to parent_id;
ALTER TABLE schema_usages DROP COLUMN child_id;
ALTER TABLE schema_usages RENAME COLUMN newfkc to child_id;

--dropping old pk, renaming new pk  and adding pk constraint in parent table
ALTER TABLE schemas DROP COLUMN schema_id;
ALTER TABLE schemas RENAME COLUMN newpk to schema_id;
ALTER TABLE schemas ADD CONSTRAINT schema_pkey PRIMARY KEY(schema_id);

--add fk and not null constraints to all child tables
ALTER TABLE data_source_links ADD CONSTRAINT data_source_links_schema_id_fkey FOREIGN KEY (schema_id) REFERENCES schemas(schema_id);
ALTER TABLE data_source_links ALTER COLUMN schema_id SET NOT NULL;
ALTER TABLE schema_usages ADD CONSTRAINT schema_usages_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES schemas(schema_id);
ALTER TABLE schema_usages ALTER COLUMN parent_id SET NOT NULL;
ALTER TABLE schema_usages ADD CONSTRAINT schema_usages_child_id_fkey FOREIGN KEY (child_id) REFERENCES schemas(schema_id);
ALTER TABLE schema_usages ALTER COLUMN child_id SET NOT NULL;



-------Relinking data_sources

--adding uuid columns(they will replace current pks and fks)
ALTER TABLE data_sources ADD COLUMN newpk uuid;
ALTER TABLE data_source_links ADD COLUMN newfk uuid;

--initializing new pk of parent with random value and linking child tables
UPDATE data_sources SET newpk = uuid_generate_v4();
UPDATE data_source_links s SET newfk = (select newpk from data_sources c where c.data_source_id = s.data_source_id);

--dropping old fk and renaming new fk in child tables
ALTER TABLE data_source_links DROP COLUMN data_source_id;
ALTER TABLE data_source_links RENAME COLUMN newfk to data_source_id;

--dropping old pk, renaming new pk  and adding pk constraint in parent table
ALTER TABLE data_sources DROP COLUMN data_source_id;
ALTER TABLE data_sources RENAME COLUMN newpk to data_source_id;
ALTER TABLE data_sources ADD CONSTRAINT data_source_pkey PRIMARY KEY(data_source_id);

--add fk and not null constraints to all child tables
ALTER TABLE data_source_links ADD CONSTRAINT data_source_links_data_source_id_fkey FOREIGN KEY (data_source_id) REFERENCES data_sources(data_source_id);
ALTER TABLE data_source_links ALTER COLUMN data_source_id SET NOT NULL;

--add unique-name-within-catalog constraints
ALTER TABLE schemas ADD CONSTRAINT schemas_catalog_id_name_key UNIQUE (catalog_id, name);
ALTER TABLE data_sources ADD CONSTRAINT data_sources_catalog_id_name_key UNIQUE (catalog_id, name);
ALTER TABLE saved_queries ADD CONSTRAINT saved_queries_catalog_id_name_key UNIQUE (catalog_id, name);

# --- !Downs