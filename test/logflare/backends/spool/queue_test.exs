defmodule Logflare.Backends.Spool.QueueTest do
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.Queue

  setup do
    prev_spool_config = Application.get_env(:logflare, :spool)
    Application.put_env(:logflare, :spool, max_spool_health_failures: 1)

    on_exit(fn ->
      Health.report_recovery!(:queue)

      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    Health.report_recovery!(:queue)
    :ok
  end

  describe "track_health/1" do
    for result <- [
          {:error, %Mint.TransportError{reason: :nxdomain}},
          {:error, %Mint.TransportError{reason: :timeout}},
          {:error, %Mint.HTTPError{reason: :closed}},
          {:error, :pool_timeout},
          {:error, {:http_error, 503, "unavailable"}},
          {:error, %Tesla.Env{status: 500}}
        ] do
      test "#{inspect(result)} counts against the queue" do
        result = unquote(Macro.escape(result))

        assert Queue.track_health(result) == result
        refute Health.healthy?(:queue)
      end
    end

    for result <- [
          :ok,
          {:ok, []},
          {:error, {:http_error, 400, "ReceiptHandleIsInvalid"}},
          {:error, %Tesla.Env{status: 404}}
        ] do
      test "#{inspect(result)} proves the queue is reachable" do
        result = unquote(Macro.escape(result))
        Health.mark_unhealthy!(:queue)

        assert Queue.track_health(result) == result
        assert Health.healthy?(:queue)
      end
    end

    test "errors that say nothing about reachability leave the state alone" do
      Health.mark_unhealthy!(:queue)

      assert Queue.track_health({:error, "boom"}) == {:error, "boom"}
      refute Health.healthy?(:queue)
    end
  end
end
