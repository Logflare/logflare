defmodule Logflare.Source do
  use Ecto.Schema
  alias Logflare.SourceData
  alias Number.Delimit
  import Ecto.Changeset
  @default_source_api_quota 50

  defmodule Metrics do
    use Ecto.Schema

    embedded_schema do
      field :rate, :integer
      field :latest, :integer
      field :avg, :integer
      field :max, :integer
      field :buffer, :integer
      field :inserts, :integer
      field :rejected, :integer
    end
  end

  schema "sources" do
    field :name, :string
    field :token, Ecto.UUID.Atom
    field :public_token, :string
    field :avg_rate, :integer, virtual: true
    field :favorite, :boolean, default: false
    field :user_email_notifications, :boolean, default: false
    field :other_email_notifications, :string
    field :user_text_notifications, :boolean, default: false
    field :bigquery_table_ttl, :integer
    field :api_quota, :integer, default: @default_source_api_quota

    belongs_to :user, Logflare.User
    has_many :rules, Logflare.Rule
    field :metrics, :map, virtual: true
    field :has_rejected_events?, :boolean, default: false, virtual: true

    timestamps()
  end

  @doc false
  def changeset(source, attrs) do
    source
    |> cast(attrs, [
      :name,
      :token,
      :public_token,
      :avg_rate,
      :favorite,
      :user_email_notifications,
      :other_email_notifications,
      :user_text_notifications,
      :bigquery_table_ttl,
      :api_quota
    ])
    |> validate_required([:name, :token])
    |> unique_constraint(:name)
    |> unique_constraint(:public_token)
    |> validate_min_avg_source_rate(:avg_rate)
  end

  def update_metrics_latest(%__MODULE__{token: token} = source) do
    import SourceData
    rejected_count = Logs.Rejected.get_by_source(source)

    metrics =
      %Metrics{
        rate: get_rate(token),
        latest: get_latest_date(token),
        avg: get_avg_rate(token),
        max: get_max_rate(token),
        buffer: get_buffer(token),
        inserts: get_total_inserts(token),
        rejected: rejected_count
      }
      |> Map.from_struct()
      |> Enum.map(fn
        {k, v} when k in ~w[rate latest avg max buffer inserts]a ->
          {k, Delimit.number_to_delimited(v)}

        x ->
          x
      end)
      |> Map.new()

    %{source | metrics: metrics, has_rejected?: rejected_count > 0}
  end

  def validate_min_avg_source_rate(changeset, field, options \\ []) do
    validate_change(changeset, field, fn _, avg_rate ->
      case avg_rate >= 1 do
        true ->
          []

        false ->
          [{field, options[:message] || "Average events per second must be at least 1"}]
      end
    end)
  end
end
