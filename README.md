# pgexplode

A utility for exploding a PostgreSQL table (and any related data) into separate schemas.


## Example

Imagine the following table structure and data in a database named `exploder_test`:

```sql
CREATE TABLE tenant (id serial PRIMARY KEY, slug varchar);
CREATE TABLE related (id serial PRIMARY KEY, tenant_id integer NOT NULL REFERENCES tenant(id), value varchar);
INSERT INTO tenant (id, slug) VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO related (tenant_id, value) VALUES
    (1, 'alpha-value-1'),
    (1, 'alpha-value-2'),
    (1, 'alpha-value-3'),
    (2, 'beta-value-1'),
    (2, 'beta-value-2'),
    (2, 'beta-value-3'),
    (2, 'beta-value-4'),
    (2, 'beta-value-5')
;
```

Running the following command:

```
python -m pgexplode -d exploder_test -t tenant -s slug
```

Would create two schemas, `alpha` and `beta` and copy the table data as follows:

```
+ alpha
  ~ tenant: 1
  ~ related: 3
+ beta
  ~ tenant: 1
  ~ related: 5
```

## Outputting/Debugging SQL

Adding an `--sql` flag to the command above will output the SQL being run, which can be helpful when tweaking or
debugging:

```sql
-- alpha
DROP SCHEMA IF EXISTS "alpha" CASCADE;
CREATE SCHEMA "alpha";
CREATE TABLE "alpha".tenant (LIKE public.tenant INCLUDING ALL);
INSERT INTO "alpha".tenant (SELECT * FROM tenant WHERE id = 1);
CREATE TABLE "alpha".related (LIKE public.related INCLUDING ALL);
INSERT INTO "alpha".related (SELECT related.* FROM related JOIN tenant ON related.tenant_id = tenant.id WHERE tenant.id = 1);
CREATE SEQUENCE "alpha".related_id_seq;
ALTER SEQUENCE "alpha".related_id_seq OWNED BY "alpha".related.id;
ALTER TABLE "alpha".related ALTER id SET DEFAULT nextval('alpha.related_id_seq'::regclass);
CREATE SEQUENCE "alpha".tenant_id_seq;
ALTER SEQUENCE "alpha".tenant_id_seq OWNED BY "alpha".tenant.id;
ALTER TABLE "alpha".tenant ALTER id SET DEFAULT nextval('alpha.tenant_id_seq'::regclass);
ALTER TABLE "alpha".related ADD CONSTRAINT related_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES "alpha".tenant (id);

-- beta
DROP SCHEMA IF EXISTS "beta" CASCADE;
CREATE SCHEMA "beta";
CREATE TABLE "beta".tenant (LIKE public.tenant INCLUDING ALL);
INSERT INTO "beta".tenant (SELECT * FROM tenant WHERE id = 2);
CREATE TABLE "beta".related (LIKE public.related INCLUDING ALL);
INSERT INTO "beta".related (SELECT related.* FROM related JOIN tenant ON related.tenant_id = tenant.id WHERE tenant.id = 2);
CREATE SEQUENCE "beta".related_id_seq;
ALTER SEQUENCE "beta".related_id_seq OWNED BY "beta".related.id;
ALTER TABLE "beta".related ALTER id SET DEFAULT nextval('beta.related_id_seq'::regclass);
CREATE SEQUENCE "beta".tenant_id_seq;
ALTER SEQUENCE "beta".tenant_id_seq OWNED BY "beta".tenant.id;
ALTER TABLE "beta".tenant ALTER id SET DEFAULT nextval('beta.tenant_id_seq'::regclass);
ALTER TABLE "beta".related ADD CONSTRAINT related_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES "beta".tenant (id);
```

You can see in addition to simply creating copies of each table, `pgexplode` is also making sure the new tables have
their own sequences for serial columns, and tables are properly re-keyed within the new schema.
