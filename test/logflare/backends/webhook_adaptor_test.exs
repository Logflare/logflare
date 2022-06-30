defmodule Logflare.Backends.WebhookAdaptorTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{LogEvent, Backends, Backends.SourceBackend}

  setup do
    source_backend = insert(:source_backend)
    {:ok, source_backend: source_backend}
  end
  test "dispatch_ingest", %{source_backend: source_backend} do
    # send the log event through rules
    log_event = %LogEvent{}
    assert :ok = Backends.dispatch_ingest(source_backend,[log_event])

    raise "not impl"
  end
end
