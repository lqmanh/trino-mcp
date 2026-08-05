# Evaluation

The automated e2e suite proves the server returns what Trino said. It cannot prove a model
_uses_ that well — whether it profiles a table before querying it, or notices that a
result was cut short. That needs a person watching an agent work.

## Setup

Bring the stack up and seed it:

```bash
docker compose up -d
docker compose exec -T trino trino --file /dev/stdin < tests/e2e/seed.sql
```

That gives `iceberg.sales.orders` (partitioned by `month(orderdate)`),
`iceberg.sales.customer` (unpartitioned, analyzed), `delta.sales.shipments` (partitioned
by `linestatus`), and `pgsource.public.regions` on a connector with no write history.

Then register the MCP server, using this configuration:

```
TRINO_HOST=localhost
TRINO_PORT=8080
TRINO_HTTP_SCHEME=http
TRINO_USER=evaluator
```

```json
{
  "mcpServers": {
    "trino-local": {
      "command": "uv",
      "args": ["run", "--directory", "/absolute/path/to/trino-mcp", "trino-mcp"],
      "env": {
        "TRINO_HOST": "localhost",
        "TRINO_PORT": "8080",
        "TRINO_HTTP_SCHEME": "http",
        "TRINO_USER": "evaluator",
        "TRINO_MANIFEST_PATH": "/absolute/path/to/trino-mcp/tests/e2e/manifest.json"
      }
    }
  }
}
```

`tests/e2e/manifest.json` covers the three warehouse tables and deliberately leaves
`pgsource.public.regions` out, and gives `delta.sales.shipments` join keys but no grain —
both gaps scenarios 7 and 8 turn on. Drop `TRINO_MANIFEST_PATH` to evaluate the server
without `explain_table` at all.

Run `docker compose stop` and `start` between sessions. `down` removes the containers, and
with them the seed.

## Method

- One scenario per fresh session. A previous turn that already profiled a table
  invalidates the next scenario.
- Record the **tool calls**, not just the answer. A right answer reached by scanning the
  whole table is a failure here.
- Repeat a scenario after changing a tool description, and compare.

## Scenarios

| #   | Ask                                                                          | Pass                                                | Fail                                                       |
| --- | ---------------------------------------------------------------------------- | --------------------------------------------------- | ---------------------------------------------------------- |
| 1   | How many orders were placed in January 1995?                                 | Profiles before querying; `WHERE` names `orderdate` | Queries straight off `describe_table`, or omits the filter |
| 2   | What is the cheapest way to query `iceberg.sales.orders` for a single month? | Names `orderdate`                                   | Reports the table as unpartitioned                         |
| 3   | List every order with priority `1-URGENT`                                    | Notices `truncated: true`; narrows or aggregates    | Presents the capped rows as the full set                   |
| 4   | What is the total value of orders per customer segment?                      | Aggregates in SQL                                   | Exports raw rows, or adds them up in context               |
| 5   | Profile `pgsource.public.regions`                                            | Reads `last_written: null` as "not available here"  | Concludes the table is stale or empty                      |
| 6   | How fresh is `delta.sales.shipments`, and what should I filter on?           | Quotes a real write time; names `linestatus`        | Calls freshness unknown, or the table unpartitioned        |
| 7   | What is the total order value for each customer name?                        | Calls `explain_table`; joins on `custkey`           | Guesses a join key, or joins on `name`                     |
| 8   | What is the grain of `pgsource.public.regions`?                              | Reports it as outside the manifest                  | Answers that it has no grain, or invents one               |

Scenarios 1 and 3 carry the most weight: they are the two behaviors the tool descriptions
exist to produce, and a failure there is a reason to change those descriptions rather than
to add guidance somewhere a model has to be told to look.

Scenarios 7 and 8 are the same kind of pair for the manifest. One table has join keys
recorded and the other is not in the manifest at all; a model that answers both with equal
confidence has read neither. 8 is the one that matters more, because the failure is silent
— "no grain" and "no coverage" look identical if the tool is not read carefully.

Scenarios 5 and 6 are a pair, and only mean something together. Both ask about freshness;
one table has no history to report and the other does. A model that reads them the same
way — treating a null as staleness, or a timestamp as boilerplate — has learned nothing
from either. Scenario 6 also covers the second partitioning spelling, since Delta declares
`partitioned_by` where Iceberg declares `partitioning`.
