import argparse
import asyncio

import asyncpg

__version_info__ = (0, 1, 0)
__version__ = ".".join(str(i) for i in __version_info__)


TABLE_SQL = """
    SELECT t.table_name, array_agg(kcu.column_name) AS pk_cols
    FROM information_schema.tables t
    JOIN information_schema.table_constraints tc
        ON tc.table_schema = t.table_schema
        AND tc.table_name = t.table_name
        AND tc.constraint_type = 'PRIMARY KEY'
    JOIN information_schema.key_column_usage kcu
        ON kcu.constraint_name = tc.constraint_name
        AND kcu.constraint_schema = tc.constraint_schema
    WHERE t.table_type = 'BASE TABLE' AND t.table_schema = 'public'
    GROUP BY t.table_name
"""

FK_SQL = """
    SELECT
        tc.constraint_name,
        tc.table_schema,
        tc.table_name,
        kcu.column_name,
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        c.is_nullable = 'YES' AS nullable
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        JOIN information_schema.columns AS c
            ON c.table_schema = tc.table_schema
            AND c.table_name = tc.table_name
            AND c.column_name = kcu.column_name
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
"""


async def copy_table(db, table, schema, select):
    """
    Creates a copy of the table in the new schema, and copies any related data into it.
    """
    await db.execute(
        "CREATE TABLE {schema}.{table} (LIKE public.{table} INCLUDING ALL)".format(table=table, schema=schema)
    )
    status = await db.execute(
        "INSERT INTO {schema}.{table} ({select})".format(table=table, schema=schema, select=select)
    )
    return int(status.split()[-1])


async def restore_keys(db, schema):
    """
    Since CREATE TABLE LIKE does not copy foreign key constraints, we have to add them back manually.
    """
    for row in await db.fetch(FK_SQL):
        await db.execute(
            "ALTER TABLE {}.{} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}.{} ({})".format(
                schema,
                row["table_name"],
                row["constraint_name"],
                row["column_name"],
                schema,
                row["foreign_table_name"],
                row["foreign_column_name"],
            )
        )


async def build_graph(db, skip=None):
    """
    Builds a graph of the database, including PK colunns, and all FK references.
    """
    graph = {}
    for row in await db.fetch(TABLE_SQL):
        if skip and row["table_name"] in skip:
            continue
        graph[row["table_name"]] = {"pks": row["pk_cols"], "fks": {}}
    for row in await db.fetch(FK_SQL):
        if skip and (row["table_name"] in skip or row["foreign_table_name"] in skip):
            continue
        graph[row["table_name"]]["fks"].setdefault(row["foreign_table_name"], []).append(
            (row["column_name"], row["foreign_column_name"], row["nullable"])
        )
    return graph


def find_joins(table, root, graph, path=None):
    """
    Finds the shortest path of INNER joins (i.e. non-nullable FK colummns) from table to root.
    """
    if table == root:
        return path
    if path is None:
        path = []
    candidates = []
    for parent, columns in graph[table]["fks"].items():
        if parent == table:
            continue
        for from_col, to_col, nullable in columns:
            if not nullable:
                found = find_joins(parent, root, graph, path + [(parent, from_col, to_col)])
                if found:
                    candidates.append(found)
    candidates.sort(key=len)
    return candidates[0] if candidates else None


async def table_data(db, graph, root, root_id):
    """
    Yields each table along with a SELECT statement of the data that should be copied for that table, based on how it
    relates to the root table (and the root_id record specifically).
    """
    seen = set()
    while len(seen) < len(graph):
        for child, info in graph.items():
            if child in seen:
                continue
            if set(info["fks"]).difference({child}) <= seen:
                # I originally wrote this so that tables would be yielded in an order that ensured any related tables
                # and data would have already been copied. Not sure this is necessary anymore, since FK constraints
                # are not copied as part of CREATE TABLE LIKE.
                seen.add(child)
                pk = info["pks"][0]
                joins = find_joins(child, root, graph)
                if joins:
                    parts = ["SELECT {}.* FROM {}".format(child, child)]
                    last = child
                    for parent, from_col, to_col in joins:
                        parts.append(
                            "JOIN {table} ON {on}".format(
                                table=parent, on="{}.{} = {}.{}".format(last, from_col, parent, to_col)
                            )
                        )
                        last = parent
                    parts.append("WHERE {}.{} = {}".format(root, pk, root_id))
                    yield child, " ".join(parts)
                elif child == root:
                    yield child, "SELECT * FROM {} WHERE {} = {}".format(root, pk, root_id)
                else:
                    yield child, "SELECT * FROM {}".format(child)


async def explode(opts):
    db = await asyncpg.connect(database=opts.dbname)

    graph = await build_graph(db)
    pk = graph[opts.table]["pks"][0]

    if graph[opts.table]["fks"]:
        print("Warning: Root table has FK links!")

    where = ""
    params = []
    if opts.ids:
        in_clause = ", ".join("${}".format(i + 1) for i in range(len(opts.ids)))
        where = " WHERE {} IN ({})".format(pk, in_clause)
        params = [int(i) if i.isdigit() else i for i in opts.ids]

    for row in await db.fetch("SELECT * FROM {}{}".format(opts.table, where), *params):
        schema = row[opts.schema] if opts.schema else "{}_{}".format(opts.table, row[pk])

        await db.execute("DROP SCHEMA IF EXISTS {} CASCADE".format(schema))
        await db.execute("CREATE SCHEMA {}".format(schema))
        print("+", schema, flush=True)

        async for table, select in table_data(db, graph, opts.table, row[pk]):
            print("  ~", table, end=": ", flush=True)
            num = await copy_table(db, table, schema, select)
            print(num, flush=True)

        # TODO: create id sequences, update pk defaults
        await restore_keys(db, schema)

    await db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Explode a PostgreSQL table (and any related data) into separate schemas."
    )
    parser.add_argument("-d", "--dbname", required=True, help="Database name to connect to")
    parser.add_argument("-t", "--table", required=True, help="The table to explode schemas based on")
    parser.add_argument("-s", "--schema", help="Column of the base table to use for schema names")
    parser.add_argument("-i", "--id", action="append", dest="ids", metavar="ROW_ID", help="Specific row(s) to explode")
    asyncio.run(explode(parser.parse_args()))
