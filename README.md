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
