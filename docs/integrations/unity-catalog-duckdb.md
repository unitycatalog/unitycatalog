# Unity Catalog DuckDB Integration

## Operate on Delta tables with DuckDB

To try operating on Delta tables with DuckDB, you will have to [install it](https://duckdb.org/docs/installation/)
(at least version 1.0).

Let's start DuckDB and install a couple of extensions. To start DuckDB, run the command `duckdb` in the terminal.

Then, in the DuckDB shell, run the following commands:

```sh
install uc_catalog from core_nightly;
load uc_catalog;
install delta;
load delta;
```

If you have installed these extensions before, you may have to run `update extensions` and restart DuckDB
for the following steps to work.

Now that we have DuckDB all set up, let's try connecting to UC by specifying a secret.

```sh
CREATE SECRET (
      TYPE UC,
      TOKEN 'not-used',
      ENDPOINT 'http://127.0.0.1:8080',
      AWS_REGION 'us-east-2'
 );
```

You should see it print a short table saying `Success` = `true`. Next we can attach the `unity` catalog to DuckDB.

```sh
ATTACH 'unity' AS unity (TYPE UC_CATALOG);
```

Now we are ready to query. Try the following

```sql
SHOW ALL TABLES;
SELECT * from unity.default.numbers;
```

You should see the tables listed and the contents of the `numbers` table printed.
To quit DuckDB, run the command `Ctrl+D` or type `.exit` in the DuckDB shell.
