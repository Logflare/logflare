defmodule Logflare.Endpoints.Query do
  @moduledoc false

  use TypedEctoSchema

  import Ecto.Changeset
  import Logflare.Utils.Guards

  require Logger

  alias Ecto.Changeset
  alias Logflare.Alerting
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Endpoints
  alias Logflare.SingleTenant
  alias Logflare.Sql
  alias Logflare.User

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
             :enable_auth,
             :labels
           ]}
  typed_schema "endpoint_queries" do
    field(:token, Ecto.UUID, autogenerate: true)
    field(:name, :string)
    field(:query, :string)
    field(:description, :string)
    field(:language, Ecto.Enum, values: [:bq_sql, :ch_sql, :lql, :pg_sql], default: :bq_sql)
    field(:source_mapping, :map)
    field(:sandboxable, :boolean)
    field(:cache_duration_seconds, :integer, default: 3_600)
    field(:proactive_requerying_seconds, :integer, default: 1_800)
    field(:max_limit, :integer, default: 1_000)
    field(:enable_auth, :boolean, default: true)
    field(:labels, :string)
    field(:parsed_labels, :map, virtual: true)
    field(:metrics, :map, virtual: true)

    belongs_to(:user, User)
    belongs_to(:backend, Backend)

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
      :description,
      :backend_id,
      :labels
    ])
    |> infer_language_from_backend()
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
      :description,
      :backend_id,
      :labels
    ])
    |> infer_language_from_backend()
    |> validate_query(:query)
    |> default_validations()
    |> update_source_mapping()
  end

  def default_validations(changeset) do
    changeset
    |> validate_required([:name, :query, :user, :language])
    |> unique_constraint(:name, name: :endpoint_queries_name_index)
    |> unique_constraint(:token)
    |> validate_number(:max_limit, greater_than: 0, less_than: 10_001)
  end

  def validate_query(changeset, field) when is_atom_value(field) do
    language = Changeset.get_field(changeset, :language, :bq_sql)
    user = get_field(changeset, :user)
    endpoint_name = get_field(changeset, :name)

    # TODO abstract out to separate Query context

    queries =
      if user do
        endpoints =
          Endpoints.list_endpoints_by(user_id: user.id)
          |> Enum.filter(&(&1.id != endpoint_name))

        alerts = Alerting.list_alert_queries_by_user_id(user.id)
        endpoints ++ alerts
      else
        []
      end

    validate_change(changeset, field, fn field, value ->
      {:ok, expanded_query} =
        Sql.expand_subqueries(
          language,
          value,
          queries
        )

      case Sql.transform(language, expanded_query, user) do
        {:ok, _} -> []
        {:error, error} -> [{field, error}]
      end
    end)
  end

  # Only update source mapping if there are no errors
  def update_source_mapping(%Changeset{errors: [], changes: %{query: query}} = changeset)
      when is_non_empty_binary(query) do
    case Sql.sources(query, get_field(changeset, :user)) do
      {:ok, source_mapping} -> put_change(changeset, :source_mapping, source_mapping)
      {:error, error} -> add_error(changeset, :query, error)
    end
  end

  def update_source_mapping(changeset), do: changeset

  @spec map_backend_to_language(Backend.t(), supabase_mode :: boolean()) ::
          :bq_sql | :ch_sql | :pg_sql
  def map_backend_to_language(%Backend{type: :clickhouse}, _supabase_mode), do: :ch_sql
  def map_backend_to_language(%Backend{type: :postgres}, false), do: :pg_sql
  def map_backend_to_language(_backend, _supabase_mode), do: :bq_sql

  @spec infer_language_from_backend(%Changeset{}) :: %Changeset{}
  defp infer_language_from_backend(%Changeset{} = changeset) do
    case get_change(changeset, :language) do
      nil ->
        case get_field(changeset, :backend_id) do
          nil ->
            # Default to BigQuery when no backend is selected
            put_change(changeset, :language, :bq_sql)

          backend_id ->
            backend = Backends.get_backend(backend_id)
            language = map_backend_to_language(backend, SingleTenant.supabase_mode?())
            put_change(changeset, :language, language)
        end

      _ ->
        changeset
    end
  end

  @doc """
  Replaces a query with latest source names.
  """
  @spec map_query_sources(__MODULE__.t()) :: __MODULE__.t()
  def map_query_sources(
        %__MODULE__{query: query, source_mapping: source_mapping, user_id: user_id} = q
      ) do
    case Sql.source_mapping(query, user_id, source_mapping) do
      {:ok, query} ->
        Map.put(q, :query, query)

      {:error, _} = err ->
        Logger.error("Could not map source query, #{inspect(err)}", error_string: inspect(q))
        q
    end
  end
end
