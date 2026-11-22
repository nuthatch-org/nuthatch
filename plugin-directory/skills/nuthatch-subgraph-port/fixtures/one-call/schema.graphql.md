Source file `nuthatch-subgraph-port/fixtures/one-call/schema.graphql`, shown as Markdown because the plugin directory takes no source files.

```graphql
"""RFC-0044 S2: one eth_call, one exact field. Dropping the call must drop the [[calls]] stanza."""
type Token @entity {
  id: ID!
  symbol: String!
}
```
