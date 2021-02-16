defmodule Logflare.Changefeeds do
  use Logflare.Commons

  alias Logflare.Changefeeds.ChangefeedSubscription

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

  def maybe_insert_virtual(%schema{} = struct) do
    virtual_schema = Module.concat(schema, Virtual)

    if Code.ensure_loaded?(virtual_schema) do
      virtual_struct = virtual_schema.changefeed_changeset(struct)

      {:ok, _virtual_struct} =
        MemoryRepo.insert(virtual_struct, on_conflict: :replace_all, conflict_target: :id)

      :ok
    else
      :ok
    end
  end

  def maybe_delete_virtual(struct_or_changeset) do
    schema =
      case struct_or_changeset do
        %Ecto.Changeset{data: %schema{}} -> schema
        %schema{} -> schema
      end

    virtual_schema = Module.concat(schema, Virtual)

    if Code.ensure_loaded?(virtual_schema) do
      {:ok, _deleted} = MemoryRepo.delete(struct_or_changeset)

      :ok
    else
      :ok
    end
  end

  def replace_assocs_with_nils(xs, schema) when is_list(xs) do
    for x <- xs do
      replace_assocs_with_nils(x, schema)
    end
  end

  def replace_assocs_with_nils(x, schema) when is_struct(x) do
    alias Ecto.Association.{BelongsTo, Has}

    for af <- EctoSchemaReflection.associations(schema), reduce: x do
      acc ->
        val = Map.get(acc, af)

        if val && is_struct(val) && val.__struct__ == Ecto.Association.NotLoaded do
          val =
            case schema.__schema__(:association, af) do
              %BelongsTo{cardinality: :one} -> nil
              %Has{cardinality: :one} -> nil
              %Has{cardinality: :many} -> []
            end

          Map.replace!(acc, af, val)
        else
          acc
        end
    end
  end

  def replace_assocs_with_nils(x) when is_struct(x) do
    %schema{} = x
    replace_assocs_with_nils(x, schema)
  end

  def drop_assoc_fields(x) when is_struct(x) do
    Map.drop(x, EctoSchemaReflection.associations(x))
  end
end
