defmodule Logflare.GenSingletonTest do
  use ExUnit.Case
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

      assert GenServer.whereis(__MODULE__.TestGenserver)
      assert GenSingleton.get_pid(pid1)
      assert GenSingleton.get_pid(pid1) == GenSingleton.get_pid(pid2)

      Process.exit(pid1, :kill)
      refute Process.alive?(pid1)
      :timer.sleep(200)
      assert pid = GenSingleton.get_pid(pid2)
      assert GenServer.whereis(__MODULE__.TestGenserver) == pid
    end
  end
end
