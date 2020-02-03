defmodule Logflare.SystemMetric do
  use TypedEctoSchema
  import Ecto.Changeset

  typed_schema "system_metrics" do
    field :all_logs_logged, :integer
    field :node, :string

    timestamps()
  end

  def changeset(system_metric, attrs) do
    system_metric
    |> cast(attrs, [:all_logs_logged, :node])
  end
end
