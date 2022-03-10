defmodule Logflare.Endpoint.Query do
  use Ecto.Schema
  import Ecto.Changeset

  schema "endpoint_queries" do
    field :token, Ecto.UUID
    field :name, :string
    field :query, :string
    field :source_mapping, :map
    field :sandboxable, :boolean
    field :cache_duration_seconds, :integer, default: 3_600
    field :proactive_requerying_seconds, :integer, default: 1_800
    field :max_limit, :integer, default: 1_000

    belongs_to :user, Logflare.User

    timestamps()
  end

  @doc false
  def changeset(query, attrs) do
    query
    |> cast(attrs, [
      :name,
      :token,
      :query,
      :sandboxable,
      :cache_duration_seconds,
      :proactive_requerying_seconds,
      :max_limit
    ])
    |> validate_required([:name, :token, :query])
  end

  def update_by_user_changeset(query, attrs) do
    query
    |> cast(attrs, [
      :name,
      :token,
      :query,
      :sandboxable,
      :cache_duration_seconds,
      :proactive_requerying_seconds,
      :max_limit
    ])
    |> default_validations()
    |> update_source_mapping()
  end

  def default_validations(changeset) do
    changeset
    |> validate_required([:name, :token, :query, :user])
    |> validate_query(:query)
    |> unique_constraint(:name, name: :endpoint_queries_name_index)
    |> unique_constraint(:token)
    |> validate_number(:max_limit, greater_than: 0, less_than: 10_001)
  end

  def validate_query(changeset, field) when is_atom(field) do
    validate_change(changeset, field, fn field, value ->
      case Logflare.SQL.transform(value, get_field(changeset, :user)) do
        {:ok, _} ->
          []

        {:error, error} ->
          [{field, error}]
      end
    end)
  end

  def update_source_mapping(changeset) do
    if Enum.empty?(changeset.errors) do
      # Only update source mapping if there are no errors
      query = get_field(changeset, :query)

      if query do
        case Logflare.SQL.sources(query, get_field(changeset, :user)) do
          {:ok, source_mapping} ->
            put_change(changeset, :source_mapping, source_mapping)

          {:error, error} ->
            add_error(changeset, :query, error)
        end
      else
        changeset
      end
    else
      changeset
    end
  end

  def map_query(%__MODULE__{query: query, source_mapping: source_mapping, user_id: user_id} = q) do
    case Logflare.SQL.source_mapping(query, user_id, source_mapping) do
      {:ok, query} ->
        Map.put(q, :query, query)

      {:error, _} ->
        q
    end
  end
end
