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
        "TRINO_USER": "evaluator"
      }
    }
  }
}
```

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

Scenarios 1 and 3 carry the most weight: they are the two behaviours the tool descriptions
exist to produce, and a failure there is a reason to change those descriptions rather than
to add guidance somewhere a model has to be told to look.

Scenarios 5 and 6 are a pair, and only mean something together. Both ask about freshness;
one table has no history to report and the other does. A model that reads them the same
way — treating a null as staleness, or a timestamp as boilerplate — has learned nothing
from either. Scenario 6 also covers the second partitioning spelling, since Delta declares
`partitioned_by` where Iceberg declares `partitioning`.
