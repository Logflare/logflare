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
        start_supervised!({GenSingleton, interval: 300, child_spec: __MODULE__.TestGenserver},
          id: :first,
          restart: :temporary
        )

      :timer.sleep(400)

      pid2 =
        start_supervised!({GenSingleton, interval: 100, child_spec: __MODULE__.TestGenserver},
          id: :second,
          restart: :temporary
        )

      assert GenServer.whereis(__MODULE__.TestGenserver)

      assert GenSingleton.count_children(pid1) == 1
      assert GenSingleton.count_children(pid2) == 0

      Process.exit(pid1, :kill)
      refute Process.alive?(pid1)
      :timer.sleep(200)
      assert GenSingleton.count_children(pid2) == 1
    end
  end
end
