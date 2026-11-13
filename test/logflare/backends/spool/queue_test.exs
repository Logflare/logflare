defmodule Logflare.Backends.Spool.QueueTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.Queue
  alias Logflare.Backends.Spool.Queue.PubSub

  setup do
    prev_spool_config = Application.get_env(:logflare, :spool, [])
    Application.put_env(:logflare, :spool, max_spool_health_failures: 1)

    on_exit(fn ->
      Health.report_recovery!(:queue)

      Application.put_env(:logflare, :spool, prev_spool_config)
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
        Health.report_failure!(:queue)

        assert Queue.track_health(result) == result
        assert Health.healthy?(:queue)
      end
    end

    for reason <- [:nxdomain, :timeout, :econnrefused] do
      test "Pub/Sub's #{inspect(reason)} transport error through Tesla counts against the queue" do
        reason = unquote(reason)

        stub(Finch, :request, fn _req, _name, _opts ->
          {:error, %Mint.TransportError{reason: reason}}
        end)

        assert {:error, ^reason} = PubSub.publish("projects/p/topics/t", "body")
        refute Health.healthy?(:queue)
      end
    end

    test "errors that say nothing about reachability leave the state alone" do
      Health.report_failure!(:queue)

      assert Queue.track_health({:error, "boom"}) == {:error, "boom"}
      refute Health.healthy?(:queue)
    end
  end
end
