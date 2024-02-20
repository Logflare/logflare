defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  alias Logflare.Source.BigQuery.Pipeline

  @behaviour Logflare.Backends.Adaptor

  @impl Logflare.Backends.Adaptor
  def start_link(source_backend) do
    Agent.start_link(fn -> source_backend end)
  end

  @impl Logflare.Backends.Adaptor
  def ingest(pid, messages) do
    {source, backend} = Agent.get(pid, fn state -> state end)

    context = %{
      source_token: source.token,
      bigquery_project_id: backend.config.bigquery_project_id,
      bigquery_dataset_id: backend.config.bigquery_dataset_id
    }

    _ = Enum.map(messages, &Pipeline.process_data(&1.data))

    Pipeline.stream_batch(context, messages)
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
