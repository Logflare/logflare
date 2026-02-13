defmodule Logflare.FetchQueries.FetchQuery do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Backends.Backend
  alias Logflare.Sources.Source
  alias Logflare.User

  @derive {Jason.Encoder,
           only: [
             :id,
             :external_id,
             :name,
             :description,
             :language,
             :query,
             :cron,
             :enabled,
             :backend_id,
             :source_id
           ]}

  typed_schema "fetch_queries" do
    field :name, :string
    field :description, :string
    field :external_id, Ecto.UUID, autogenerate: true
    field :language, Ecto.Enum, values: [:bq_sql, :pg_sql, :lql, :jsonpath], default: :bq_sql
    field :query, :string
    field :cron, :string
    field :source_mapping, :map
    field :enabled, :boolean, default: true

    belongs_to :user, User
    belongs_to :backend, Backend
    belongs_to :source, Source

    timestamps()
  end

  @doc false
  def changeset(fetch_query, attrs) do
    fetch_query
    |> cast(attrs, [
      :name,
      :description,
      :language,
      :query,
      :cron,
      :backend_id,
      :source_id,
      :user_id,
      :enabled
    ])
    |> validate_required([:name, :cron, :language, :source_id, :user_id])
    |> validate_query_by_backend_type()
    |> validate_cron()
    |> unique_constraint(:external_id)
    |> unique_constraint(:name, name: :fetch_queries_user_id_name_index)
    |> foreign_key_constraint(:backend_id)
    |> foreign_key_constraint(:source_id)
    |> foreign_key_constraint(:user_id)
  end

  defp validate_query_by_backend_type(%{valid?: false} = changeset), do: changeset

  defp validate_query_by_backend_type(changeset) do
    backend_id = get_field(changeset, :backend_id)
    language = get_field(changeset, :language)

    if backend_id do
      # Load backend to check type
      backend = Logflare.Backends.get_backend(backend_id)

      case backend && backend.type do
        :webhook ->
          # Webhook: query optional, or JSONPath if language is :jsonpath
          if language == :jsonpath do
            validate_required(changeset, [:query])
          else
            changeset
          end

        :bigquery ->
          # BigQuery: query required
          validate_required(changeset, [:query])

        _ ->
          add_error(changeset, :backend_id, "Backend type not supported for fetch queries")
      end
    else
      changeset
    end
  end

  defp validate_cron(%{valid?: false} = changeset), do: changeset

  defp validate_cron(changeset) do
    validate_change(changeset, :cron, fn :cron, cron ->
      with {:ok, expr} <- Crontab.CronExpression.Parser.parse(cron),
           [first, second | _] <- Crontab.Scheduler.get_next_run_dates(expr) |> Enum.take(2),
           true <- NaiveDateTime.diff(first, second, :minute) <= -1 do
        []
      else
        [] -> [cron: "not enough run dates"]
        false -> [cron: "can only trigger up to 1 minute intervals"]
        {:error, msg} -> [cron: msg]
      end
    end)
  end
end
