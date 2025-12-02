defmodule Logflare.GenSingletonTest do
  use Logflare.DataCase, async: false
  alias Logflare.GenSingleton

  defmodule TestGenserver do
    use GenServer, restart: :temporary

    def start_link(args) do
      GenServer.start_link(__MODULE__, args, name: __MODULE__)
    end

    @impl GenServer
    def init(args) do
      {:ok, args}
    end
  end

  defmodule TestSupervisor do
    use Supervisor

    def start_link(args) do
      Supervisor.start_link(__MODULE__, args, name: __MODULE__)
    end

    @impl Supervisor
    def init(_args) do
      children = []
      Supervisor.init(children, strategy: :one_for_one)
    end
  end

  describe "start_link/1" do
    test "starts the GenServer with valid arguments" do
      refute GenServer.whereis(__MODULE__.TestGenserver)

      pid1 =
        start_supervised!({GenSingleton, child_spec: __MODULE__.TestGenserver},
          id: :first
        )

      pid2 =
        start_supervised!({GenSingleton, child_spec: __MODULE__.TestGenserver},
          id: :second
        )

      TestUtils.retry_assert(fn ->
        assert GenServer.whereis(__MODULE__.TestGenserver)
        assert GenSingleton.get_pid(pid1)
        assert GenSingleton.get_pid(pid1) == GenSingleton.get_pid(pid2)
      end)

      Process.exit(pid1, :kill)
      refute Process.alive?(pid1)
      :timer.sleep(200)
      assert pid = GenSingleton.get_pid(pid2)
      assert GenServer.whereis(__MODULE__.TestGenserver) == pid
    end
  end

  describe "Stopping the GenSingleton supervised process" do
    test "stop_local/1" do
      refute GenServer.whereis(__MODULE__.TestSupervisor)

      pid1 =
        start_supervised!(
          {GenSingleton, child_spec: __MODULE__.TestSupervisor},
          id: :first
        )

      TestUtils.retry_assert(fn ->
        assert GenSingleton.get_pid(pid1) == GenServer.whereis(__MODULE__.TestSupervisor)
        assert initial_pid = GenServer.whereis(__MODULE__.TestSupervisor)
        Process.info(initial_pid)
        assert :ok = GenSingleton.stop_local(pid1)
        refute Process.alive?(initial_pid)
        :timer.sleep(200)
        assert pid = GenSingleton.get_pid(pid1)
        assert pid != initial_pid
      end)
    end
  end

  describe "Watcher child_spec/1" do
    test "defaults restart to :permanent" do
      spec = Logflare.GenSingleton.Watcher.child_spec(child_spec: __MODULE__.TestGenserver)
      assert spec.restart == :permanent
    end

    test "allows restart option to be overridden" do
      spec =
        Logflare.GenSingleton.Watcher.child_spec(
          child_spec: __MODULE__.TestGenserver,
          restart: :transient
        )

      assert spec.restart == :transient
    end
  end
end
