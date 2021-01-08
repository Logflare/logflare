defmodule Logflare.MemoryRepo.Migrations do
  use Logflare.Commons
  alias Logflare.EctoSchemaReflection
  alias Logflare.BillingCounts.BillingCount
  alias Logflare.BillingAccounts.BillingAccount

  def run() do
    create_table_from_schema(:users, User)
    create_table_from_schema(:teams, Team)
    create_table_from_schema(:team_users, TeamUser)
    create_table_from_schema(:sources, Source)
    create_table_from_schema(:source_schemas, SourceSchema)
    create_table_from_schema(:saved_searches, SavedSearch)
    create_table_from_schema(:rules, Rule)
    create_table_from_schema(:billing_counts, BillingCount)
    create_table_from_schema(:billing_accounts, BillingAccount)
  end

  def create_table_from_schema(ecto_table, schema) do
    attributes =
      EctoSchemaReflection.fields(schema) ++
        EctoSchemaReflection.embeds(schema)

    :mnesia.create_table(ecto_table,
      ram_copies: [node()],
      record_name: schema,
      attributes: Enum.uniq(attributes),
      type: :ordered_set
    )
  end
end
