defmodule Logflare.Sources.Source.SupervisorTest do
  use Logflare.DataCase

  import ExUnit.CaptureLog

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.ContextCache
  alias Logflare.Google.BigQuery
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Sources.Source

  setup do
    stub(BigQuery, :init_table!, fn _, _, _, _, _, _ -> :ok end)
    insert(:plan)

    on_exit(fn ->
      for {_id, child, _, _} <- DynamicSupervisor.which_children(Backends.SourcesSup) do
        DynamicSupervisor.terminate_child(Backends.SourcesSup, child)
      end
    end)

    {:ok, user: insert(:user), token: TestUtils.gen_uuid_atom()}
  end

  describe "handle_cast/2 with :create" do
    test "logs a warning for a token that has no source", %{token: token} do
      log =
        capture_log([level: :warning], fn ->
          assert {:noreply, nil} = Source.Supervisor.handle_cast({:create, token}, nil)
        end)

      assert log =~ "Source not found on create: #{token}"
    end

    test "starts the source when the cache holds nil and the primary has the row", %{
      user: user,
      token: token
    } do
      assert Sources.Cache.get_by(token: token) == nil
      source = insert(:source, token: token, user: user)

      assert {:noreply, nil} = Source.Supervisor.handle_cast({:create, token}, nil)

      assert {:ok, _pid} = Backends.lookup(SourceSup, source)
    end

    test "logs an error when the database is unavailable", %{token: token} do
      stub(ContextCache, :apply_fun, fn _, _, _ -> {:error, :database_unavailable} end)

      log =
        capture_log(fn ->
          assert {:noreply, nil} = Source.Supervisor.handle_cast({:create, token}, nil)
        end)

      assert log =~ "Failed to start SourceSup: #{token}"
    end
  end

  describe "handle_cast/2 with :stop" do
    test "deletes the counters for a token that has no source", %{token: token} do
      Counters.increment(token)
      assert {:ok, 1} = Counters.get_inserts(token)

      assert {:noreply, nil} = Source.Supervisor.handle_cast({:stop, token}, nil)

      assert {:ok, 0} = Counters.get_inserts(token)
    end

    test "stops the source when the cache holds nil and the primary has the row", %{
      user: user,
      token: token
    } do
      assert Sources.Cache.get_by(token: token) == nil
      source = insert(:source, token: token, user: user)
      assert :ok = Backends.start_source_sup(source)
      assert {:ok, _pid} = Backends.lookup(SourceSup, source)

      assert {:noreply, nil} = Source.Supervisor.handle_cast({:stop, token}, nil)

      TestUtils.retry_assert(fn ->
        assert {:error, :not_started} = Backends.lookup(SourceSup, source)
      end)
    end

    test "deletes the counters when the database is unavailable", %{token: token} do
      stub(ContextCache, :apply_fun, fn _, _, _ -> {:error, :database_unavailable} end)
      Counters.increment(token)

      assert {:noreply, nil} = Source.Supervisor.handle_cast({:stop, token}, nil)

      assert {:ok, 0} = Counters.get_inserts(token)
    end
  end
end
