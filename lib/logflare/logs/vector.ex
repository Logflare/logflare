defmodule Logflare.Logs.Vector do
  @moduledoc """
  Takes payloadds from Vector and puts the whole payload in the `metadata` key.
  """

  @behaviour Logflare.Logs.Processor

  require Logger

  def handle_batch(batch, _source) when is_list(batch) do
    Enum.map(batch, fn x -> handle_event(x) end)
  end

  @doc """
  Handles the top level `project` field if it exists. For Supabase only. Need this field on the top level
  because we cluster by it in BigQuery.

  TODDO: The Supabase infra Vector config should should match what we expect to insert into BigQuery.
  And we should have a v2 Vector ingest endpoint which doesn't do any payload manipulation.
  """
  def handle_event(
        %{"project" => project, "timestamp" => timestamp, "message" => message} = params
      ) do
    metadata = Map.drop(params, ["message", "timestamp"])

    %{
      "message" => message,
      "metadata" => metadata,
      "timestamp" => timestamp,
      "project" => project
    }
  end

  # If a log event from vector contains a `message` let's use it.
  def handle_event(%{"timestamp" => timestamp, "message" => message} = params) do
    metadata = Map.drop(params, ["message", "timestamp"])

    %{
      "message" => message,
      "metadata" => metadata,
      "timestamp" => timestamp
    }
  end

  # If no `message` is present on the payload Jason encode the params and use that as the message in the log event feed.
  def handle_event(%{"timestamp" => timestamp} = params) do
    metadata = Map.drop(params, ["timestamp"])
    message = Jason.encode!(metadata)

    %{
      "message" => message,
      "metadata" => metadata,
      "timestamp" => timestamp
    }
  end
end
