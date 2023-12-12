defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.RecentLogsServer, as: RLS

  @behaviour Logflare.Backends.Adaptor

  @impl Logflare.Backends.Adaptor
  def start_link(source_backend) do
    Agent.start_link(fn -> source_backend end)
  end

  @impl Logflare.Backends.Adaptor
  def ingest(pid, events) do
    source_backend = Agent.get(pid, fn state -> state end)

    rls = %RLS{
      source_id: source_backend.source_id,
      bigquery_project_id: source_backend.source.user.bigquery_project_id,
      bigquery_dataset_id: source_backend.source.user.bigquery_dataset_id,
      source: source_backend.source
    }

    Pipeline.stream_batch(rls, events)
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_id, _query),
    do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{}}
    |> Ecto.Changeset.cast(params, [])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset),
    do: changeset
end
