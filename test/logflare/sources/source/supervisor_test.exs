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
  alias Logflare.Sources.Source.RateCounterServer

  setup do
    stub(BigQuery, :init_table!, fn _, _, _, _, _, _ -> :ok end)
    insert(:plan)

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
      terminate_source_sup_on_exit(source)

      assert {:noreply, nil} = Source.Supervisor.handle_cast({:create, token}, nil)

      assert {:ok, _pid} = Backends.lookup(SourceSup, source)
    end

    test "starts the source when the entry disappears during the primary read", %{
      user: user,
      token: token
    } do
      assert Sources.Cache.get_by(token: token) == nil
      insert(:source, token: token, user: user)
      source = Sources.get_by(token: token)
      terminate_source_sup_on_exit(source)
      test_pid = self()

      stub(BigQuery, :init_table!, fn _, ^token, _, _, _, _ ->
        send(test_pid, :init_table)
        :ok
      end)

      stub(Sources, :get_by, fn
        [token: ^token] -> nil
        keyword -> call_original(Sources, :get_by, [keyword])
      end)

      expect(Sources, :get_by, fn [token: ^token] ->
        {:ok, _busted} = ContextCache.bust_keys([{Sources, :not_found}])
        source
      end)

      assert {:noreply, nil} = Source.Supervisor.handle_cast({:create, token}, nil)

      assert_receive :init_table, 1_000
      assert {:ok, _pid} = Backends.lookup(SourceSup, source)
      assert :sys.get_state(Backends.via_source(source, RateCounterServer)) == token
      assert Sources.Cache.get_by(token: token) == nil
    end

    test "logs an error when the database is unavailable", %{token: token} do
      stub(ContextCache, :apply_fun, fn _, _, _ -> {:error, :database_unavailable} end)

      log =
        capture_log(fn ->
          assert {:noreply, nil} = Source.Supervisor.handle_cast({:create, token}, nil)
        end)

      assert log =~ "Failed to start SourceSup: #{token}"
    end

    test "logs an error when the primary read fails after a cached nil", %{token: token} do
      assert Sources.Cache.get_by(token: token) == nil
      stub_primary_failure()

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
      terminate_source_sup_on_exit(source)
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

    test "deletes the counters when the primary read fails after a cached nil", %{token: token} do
      assert Sources.Cache.get_by(token: token) == nil
      Counters.increment(token)
      stub_primary_failure()

      capture_log(fn ->
        assert {:noreply, nil} = Source.Supervisor.handle_cast({:stop, token}, nil)
      end)

      assert {:ok, 0} = Counters.get_inserts(token)
    end
  end

  defp stub_primary_failure do
    stub(Sources, :get_by, fn _ ->
      raise %DBConnection.ConnectionError{message: "primary is down"}
    end)
  end

  defp terminate_source_sup_on_exit(%Source{id: id} = source) do
    on_exit(fn ->
      with {:ok, pid} <- Backends.lookup(SourceSup, source) do
        partition = {:via, PartitionSupervisor, {Backends.SourcesSup, id}}
        assert :ok = DynamicSupervisor.terminate_child(partition, pid)
      end
    end)
  end
end
