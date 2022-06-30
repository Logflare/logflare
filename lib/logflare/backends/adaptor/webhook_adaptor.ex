defmodule Logflare.Backends.Adaptor.WebhookAdaptor do
  @moduledoc false
  alias Logflare.Backends.Adaptor
  @behaviour Adaptor

  @impl Adaptor
  def ingest(log_events) do
    :ok
  end

  @impl Adaptor
  def execute_query(query) do
    {:ok, []}
  end
end
