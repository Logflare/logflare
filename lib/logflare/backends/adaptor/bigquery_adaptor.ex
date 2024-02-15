defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  alias Logflare.Source.BigQuery.Pipeline

  @behaviour Logflare.Backends.Adaptor

  @impl Logflare.Backends.Adaptor
  def start_link(source_backend) do
    Agent.start_link(fn -> source_backend end)
  end

  @impl Logflare.Backends.Adaptor
  def ingest(pid, events) do
    source_backend = Agent.get(pid, fn state -> state end)

    context = %{
      source_token: source_backend.source.token,
      bigquery_project_id: source_backend.source.user.bigquery_project_id,
      bigquery_dataset_id: source_backend.source.user.bigquery_dataset_id,
    }

    _ = Enum.map(events, &Pipeline.process_data(&1.data))

    Pipeline.stream_batch(context, events)
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
