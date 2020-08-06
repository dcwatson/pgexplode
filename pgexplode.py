import argparse
import asyncio
import functools

__version_info__ = (0, 1, 0)
__version__ = ".".join(str(i) for i in __version_info__)


TABLE_SQL = """
    SELECT
        t.table_name,
        array_agg(ARRAY[kcu.column_name, pg_get_serial_sequence(t.table_name, kcu.column_name)]) AS pks
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


def quote_name(name):
    if name.startswith('"') and name.endswith('"'):
        return name
    return '"{}"'.format(name)


async def maybe_execute(db, log, execute, query, *args, **kwargs):
    result = "0"
    if log:
        print(query + ";")
    if execute:
        result = await db.execute(query, *args, **kwargs)
    return result


async def copy_table(executor, table, schema, select):
    """
    Creates a copy of the table in the new schema, and copies any related data into it.
    """
    await executor(
        "CREATE TABLE {schema}.{table} (LIKE public.{table} INCLUDING ALL)".format(
            schema=quote_name(schema), table=table
        )
    )
    status = await executor(
        "INSERT INTO {schema}.{table} ({select})".format(schema=quote_name(schema), table=table, select=select)
    )
    return int(status.split()[-1])


async def restore_keys(executor, schema, graph, exclude):
    """
    Since CREATE TABLE LIKE does not copy foreign key constraints, we have to add them back manually.
    """
    for table, info in graph.items():
        for parent, columns in info["fks"].items():
            if parent == table:
                continue
            for from_col, to_col, nullable, constraint_name in columns:
                parent_schema = "public" if parent in exclude else schema
                await executor(
                    "ALTER TABLE {}.{} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}.{} ({})".format(
                        quote_name(schema),
                        table,
                        constraint_name,
                        from_col,
                        quote_name(parent_schema),
                        parent,
                        to_col,
                    )
                )


async def create_sequences(executor, schema, graph, exclude):
    """
    CREATE TABLE LIKE inherits the default sequences from the source table. This creates new ones.
    """
    for table, info in graph.items():
        if table in exclude:
            continue
        for col, seq in info["pks"].items():
            if not seq:
                continue
            original_schema, name = seq.split(".")
            await executor("CREATE SEQUENCE {schema}.{name}".format(schema=quote_name(schema), name=name))
            await executor(
                "ALTER SEQUENCE {schema}.{name} OWNED BY {schema}.{table}.{col}".format(
                    schema=quote_name(schema), name=name, table=table, col=col
                )
            )
            await executor(
                "ALTER TABLE {schema}.{table} ALTER {col} SET DEFAULT nextval('{raw_schema}.{name}'::regclass)".format(
                    schema=quote_name(schema), name=name, table=table, col=col, raw_schema=schema
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
        graph[row["table_name"]] = {"pks": {a[0]: a[1] for a in row["pks"]}, "fks": {}}
    for row in await db.fetch(FK_SQL):
        if skip and (row["table_name"] in skip or row["foreign_table_name"] in skip):
            continue
        graph[row["table_name"]]["fks"].setdefault(row["foreign_table_name"], []).append(
            (row["column_name"], row["foreign_column_name"], row["nullable"], row["constraint_name"])
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
        for from_col, to_col, nullable, constraint_name in columns:
            if not nullable:
                found = find_joins(parent, root, graph, path + [(parent, from_col, to_col)])
                if found:
                    candidates.append(found)
    candidates.sort(key=len)
    return candidates[0] if candidates else None


async def table_data(graph, root, root_id):
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
                pk = list(info["pks"].keys())[0]
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
                    yield child, " ".join(parts), True
                elif child == root:
                    yield child, "SELECT * FROM {} WHERE {} = {}".format(root, pk, root_id), True
                else:
                    yield child, "SELECT * FROM {}".format(child), False


async def explode(opts):
    # Import lazily to avoid dependency issues when just checking version.
    import asyncpg

    db = await asyncpg.connect(database=opts.dbname)

    graph = await build_graph(db)
    pk = list(graph[opts.table]["pks"].keys())[0]

    if graph[opts.table]["fks"] and not opts.quiet and not opts.sql:
        print("Warning: Root table has FK links!")

    where = ""
    params = []
    if opts.ids:
        in_clause = ", ".join("${}".format(i + 1) for i in range(len(opts.ids)))
        where = " WHERE {} IN ({})".format(pk, in_clause)
        params = [int(i) if i.isdigit() else i for i in opts.ids]

    executor = functools.partial(maybe_execute, db, opts.sql, not opts.dry)

    for row in await db.fetch("SELECT * FROM {}{}".format(opts.table, where), *params):
        schema = row[opts.schema] if opts.schema else "{}_{}".format(opts.table, row[pk])

        if not opts.quiet:
            if opts.sql:
                print("--", schema, flush=True)
            else:
                print("+", schema, flush=True)

        await executor("DROP SCHEMA IF EXISTS {} CASCADE".format(quote_name(schema)))
        await executor("CREATE SCHEMA {}".format(quote_name(schema)))

        async for table, select, related in table_data(graph, opts.table, row[pk]):
            if not opts.quiet and not opts.sql:
                print("  ~", table, end=": ", flush=True)
            if table in opts.exclude:
                if not opts.quiet and not opts.sql:
                    print("EXCLUDED", flush=True)
            elif opts.minimal and not related:
                if not opts.quiet and not opts.sql:
                    print("SKIPPED", flush=True)
            else:
                num = await copy_table(executor, table, schema, select)
                if not opts.quiet and not opts.sql:
                    print(num, flush=True)

        await create_sequences(executor, schema, graph, opts.exclude)
        await restore_keys(executor, schema, graph, opts.exclude)

        if opts.sql and not opts.quiet:
            print(flush=True)

    await db.close()


def main():
    parser = argparse.ArgumentParser(
        description="Explode a PostgreSQL table (and any related data) into separate schemas."
    )
    parser.add_argument("-d", "--dbname", required=True, help="Database name to connect to")
    parser.add_argument("-t", "--table", required=True, help="The table to explode schemas based on")
    parser.add_argument("-s", "--schema", help="Column of the base table to use for schema names")
    parser.add_argument("-i", "--id", action="append", dest="ids", metavar="ROW_ID", help="Specific row(s) to explode")
    parser.add_argument("-m", "--minimal", action="store_true", default=False, help="Only copy referenced tables")
    parser.add_argument("-q", "--quiet", action="store_true", default=False, help="Run without any output")
    parser.add_argument("-n", "--dry-run", action="store_true", dest="dry", default=False, help="Dry run")
    parser.add_argument("--sql", action="store_true", default=False, help="Print the SQL that is or would be run")
    parser.add_argument("-x", "--exclude", action="append", metavar="TABLE", help="Exclude tables from explosion")
    asyncio.run(explode(parser.parse_args()))


if __name__ == "__main__":
    main()
