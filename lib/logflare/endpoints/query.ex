defmodule Logflare.Endpoints.Query do
  @moduledoc false
  use TypedEctoSchema
  import Ecto.Changeset
  require Logger

  alias Logflare.Endpoints.Query

  @derive {Jason.Encoder,
           only: [
             :id,
             :token,
             :description,
             :name,
             :query,
             :source_mapping,
             :sandboxable,
             :cache_duration_seconds,
             :proactive_requerying_seconds,
             :max_limit,
             :enable_auth
           ]}
  typed_schema "endpoint_queries" do
    field(:token, Ecto.UUID, autogenerate: true)
    field(:name, :string)
    field(:query, :string)
    field(:description, :string)
    field(:language, Ecto.Enum, values: [:bq_sql, :pg_sql, :lql], default: :bq_sql)
    field(:source_mapping, :map)
    field(:sandboxable, :boolean)
    field(:cache_duration_seconds, :integer, default: 3_600)
    field(:proactive_requerying_seconds, :integer, default: 1_800)
    field(:max_limit, :integer, default: 1_000)
    field(:enable_auth, :boolean, default: true)

    field(:metrics, :map, virtual: true)

    belongs_to(:user, Logflare.User)
    has_many(:sandboxed_queries, Query, foreign_key: :sandbox_query_id)
    belongs_to(:sandbox_query, Query)

    timestamps()
  end

  defmodule Metrics do
    @moduledoc false
    use TypedEctoSchema

    embedded_schema do
      field(:cache_count, :integer)
    end
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
      :max_limit,
      :enable_auth,
      :language,
      :description
    ])
    |> validate_required([:name, :query, :language])
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
      :max_limit,
      :enable_auth,
      :language,
      :description
    ])
    |> validate_query(:query)
    |> default_validations()
    |> update_source_mapping()
  end

  def sandboxed_endpoint_changeset(query, attrs, sandbox) do
    query
    |> cast(attrs, [
      :name,
      :token,
      :query,
      :cache_duration_seconds,
      :proactive_requerying_seconds,
      :max_limit,
      :enable_auth,
      :language,
      :description
    ])
    |> put_change(:sandboxable, false)
    |> Ecto.Changeset.put_change(:language, sandbox.language)
    |> validate_required([:sandbox_query])
    |> default_validations()
  end

  def default_validations(changeset) do
    changeset
    |> validate_required([:name, :query, :user, :language])
    |> unique_constraint(:name, name: :endpoint_queries_name_index)
    |> unique_constraint(:token)
    |> validate_number(:max_limit, greater_than: 0, less_than: 10_001)
  end

  def validate_query(changeset, field) when is_atom(field) do
    language = Ecto.Changeset.get_field(changeset, :language, :bq_sql)

    validate_change(changeset, field, fn field, value ->
      case Logflare.Sql.transform(language, value, get_field(changeset, :user)) do
        {:ok, _} -> []
        {:error, error} -> [{field, error}]
      end
    end)
  end

  # Only update source mapping if there are no errors
  def update_source_mapping(%Ecto.Changeset{errors: [], changes: %{query: query}} = changeset)
      when is_binary(query) do
    case Logflare.Sql.sources(query, get_field(changeset, :user)) do
      {:ok, source_mapping} -> put_change(changeset, :source_mapping, source_mapping)
      {:error, error} -> add_error(changeset, :query, error)
    end
  end

  def update_source_mapping(changeset), do: changeset

  @doc """
  Replaces a query with latest source names.
  """
  @spec map_query_sources(Query.t()) :: Query.t()
  def map_query_sources(
        %__MODULE__{query: query, source_mapping: source_mapping, user_id: user_id} = q
      ) do
    case Logflare.Sql.source_mapping(query, user_id, source_mapping) do
      {:ok, query} ->
        Map.put(q, :query, query)

      {:error, _} = err ->
        Logger.error("Could not map source query, #{inspect(err)}", error_string: inspect(q))
        q
    end
  end
end
