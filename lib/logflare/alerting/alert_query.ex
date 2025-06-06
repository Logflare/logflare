defmodule Logflare.Alerting.AlertQuery do
  @moduledoc false
  use TypedEctoSchema
  import Ecto.Changeset
  alias Logflare.Endpoints.Query
  alias Logflare.Alerting.AlertWebhookHeader

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
             :webhook_notification_headers,
             :slack_hook_url
           ]}
  typed_schema "alert_queries" do
    field :name, :string
    field :description, :string
    field(:language, Ecto.Enum, values: [:bq_sql, :pg_sql, :lql], default: :bq_sql)
    field :query, :string
    field :cron, :string
    field :source_mapping, :map
    field :token, Ecto.UUID, autogenerate: true
    field :slack_hook_url, :string
    field :webhook_notification_url, :string
    embeds_many :webhook_notification_headers, AlertWebhookHeader, on_replace: :delete
    belongs_to :user, Logflare.User

    timestamps()
  end

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
      :webhook_notification_url
    ])
    |> cast_embed(:webhook_notification_headers, with: &webhook_header_changeset/2)
    |> validate_required([:name, :query, :cron, :language])
    |> validate_change(:cron, fn :cron, cron ->
      with {:ok, expr} <- Crontab.CronExpression.Parser.parse(cron),
           [first, second] <- Crontab.Scheduler.get_next_run_dates(expr) |> Enum.take(2),
           true <- NaiveDateTime.diff(first, second, :minute) <= -15 do
        []
      else
        false -> [cron: "can only trigger up to 15 minute intervals"]
        {:error, msg} -> [cron: msg]
      end
    end)

    # this source mapping logic is for any generic changeset
    # we implement the same columns for now,
    # can consider migrating to separate table in future.
    |> Query.update_source_mapping()
  end

  defp webhook_header_changeset(schema, params) do
    AlertWebhookHeader.changeset(schema, params)
  end
end
