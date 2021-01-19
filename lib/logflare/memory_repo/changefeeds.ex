defmodule Logflare.Changefeeds do
  use Logflare.Commons

  alias Logflare.Changefeeds.ChangefeedSubscription
  use Logflare.Commons

  defmodule ChangefeedSubscription do
    use TypedStruct

    typedstruct do
      field :table, String.t(), require: true
      field :schema, module(), require: true
      field :id_only, boolean, require: true
    end
  end

  def list_changefeed_subscriptions() do
    for config <- MemoryRepo.config(:changefeed_subscriptions) do
      case config do
        schema when is_atom(schema) ->
          source = EctoSchemaReflection.source(schema)

          %ChangefeedSubscription{
            table: source,
            schema: schema,
            id_only: false
          }

        {schema, id_only: true} when is_atom(schema) ->
          source = EctoSchemaReflection.source(schema)

          %ChangefeedSubscription{
            table: source,
            schema: schema,
            id_only: true
          }
      end
    end
  end

  def tables() do
    for schema <- MemoryRepo.config(:tables) do
      {EctoSchemaReflection.source(schema), schema}
    end
  end

  def list_changefeed_channels() do
    for chgsub <- list_changefeed_subscriptions() do
      pg_channel_name(chgsub)
    end
  end

  @spec get_changefeed_subscription_by_table(String.t()) :: ChangefeedSubscription.t() | nil
  def get_changefeed_subscription_by_table(table) when is_binary(table) do
    Enum.find(list_changefeed_subscriptions(), &(&1.table == table))
  end

  def pg_channel_name(%ChangefeedSubscription{table: table, id_only: true}) do
    "#{table}_id_only_changefeed"
  end

  def pg_channel_name(%ChangefeedSubscription{table: table, id_only: false}) do
    "#{table}_changefeed"
  end

  def trigger_name(%ChangefeedSubscription{table: table, id_only: false}) do
    "#{table}_changefeed_trigger"
  end

  def trigger_name(%ChangefeedSubscription{table: table, id_only: true}) do
    "#{table}_changefeed_id_only_trigger"
  end
end
