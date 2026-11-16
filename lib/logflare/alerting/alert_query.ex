defmodule Logflare.Alerting.AlertQuery do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  alias Crontab.CronExpression.Parser, as: CronParser
  alias Crontab.Scheduler
  alias Logflare.Endpoints.EndpointQuery

  @derive {Jason.Encoder,
           only: [
             :id,
             :token,
             :cron,
             :name,
             :description,
             :language,
             :query,
             :webhook_notification_url,
             :slack_hook_url,
             :enabled,
             :backends,
             :rule_key,
             :kind,
             :managed,
             :severity,
             :category,
             :signal_mapping,
             :scope,
             :group_by,
             :resolve_after_runs
           ]}
  typed_schema "alert_queries" do
    field :name, :string
    field :description, :string
    field :language, Ecto.Enum, values: [:bq_sql, :pg_sql, :lql], default: :bq_sql
    field :query, :string
    field :cron, :string
    field :source_mapping, :map
    field :token, Ecto.UUID, autogenerate: true
    field :slack_hook_url, :string
    field :webhook_notification_url, :string
    field :enabled, :boolean, default: true
    field :rule_key, :string
    field :kind, Ecto.Enum, values: [:scheduled, :on_match], default: :scheduled
    field :managed, :boolean, default: false
    field :severity, Ecto.Enum, values: [:critical, :major, :warning, :info], default: :warning
    field :category, Ecto.Enum, values: [:security, :performance, :health], default: :health
    field :signal_mapping, :map, default: %{}
    field :scope, :map, default: %{}
    field :group_by, {:array, :string}, default: []
    field :resolve_after_runs, :integer, default: 2

    belongs_to :user, Logflare.User

    many_to_many :backends, Logflare.Backends.Backend,
      join_through: "alert_queries_backends",
      on_replace: :delete

    timestamps()
  end

  @rule_key_format ~r/^[a-z0-9][a-z0-9_.\-]*$/

  @doc false
  def changeset(alert_query, attrs) do
    alert_query
    |> cast(attrs, [
      :name,
      :description,
      :language,
      :query,
      :cron,
      :slack_hook_url,
      :webhook_notification_url,
      :enabled,
      :rule_key,
      :kind,
      :managed,
      :severity,
      :category,
      :signal_mapping,
      :scope,
      :group_by,
      :resolve_after_runs
    ])
    |> validate_required([:name, :query, :language])
    |> validate_cron_required()
    |> validate_length(:rule_key, max: 128)
    |> validate_format(:rule_key, @rule_key_format)
    |> validate_number(:resolve_after_runs, greater_than_or_equal_to: 1)
    |> unique_constraint([:user_id, :rule_key],
      name: :alert_queries_user_id_rule_key_index,
      error_key: :rule_key
    )
    |> validate_change(:cron, fn :cron, cron ->
      with {:ok, expr} <- CronParser.parse(cron),
           [first, second] <- Scheduler.get_next_run_dates(expr) |> Enum.take(2),
           true <- NaiveDateTime.diff(first, second, :minute) <= -5 do
        []
      else
        [] -> [cron: "not enough run dates"]
        false -> [cron: "can only trigger up to 5 minute intervals"]
        {:error, msg} -> [cron: msg]
      end
    end)

    # this source mapping logic is for any generic changeset
    # we implement the same columns for now,
    # can consider migrating to separate table in future.
    |> maybe_update_source_mapping()
  end

  defp maybe_update_source_mapping(changeset) do
    case get_field(changeset, :kind) do
      :on_match -> changeset
      _ -> EndpointQuery.update_source_mapping(changeset)
    end
  end

  defp validate_cron_required(changeset) do
    case get_field(changeset, :kind) do
      :on_match -> changeset
      _ -> validate_required(changeset, [:cron])
    end
  end
end
