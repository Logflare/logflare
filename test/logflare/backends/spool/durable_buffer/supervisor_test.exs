defmodule Logflare.Backends.Spool.DurableBuffer.SupervisorTest do
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.DurableBuffer.Supervisor, as: SpoolDurableBufferSup

  setup do
    prev_spool_config = Application.get_env(:logflare, :spool)
    prev_schedulers = System.schedulers_online()

    on_exit(fn ->
      :erlang.system_flag(:schedulers_online, prev_schedulers)

      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    Application.put_env(:logflare, :spool,
      buffer: :mem,
      bucket: "test-bucket",
      storage_mod: Logflare.Backends.Spool.Storage.S3,
      queue_mod: Logflare.Backends.Spool.Queue.SQS
    )

    :ok
  end

  describe "child_spec/1 partitions" do
    test "defaults to one partition per online scheduler" do
      :erlang.system_flag(:schedulers_online, 1)
      assert partitions() == 1

      :erlang.system_flag(:schedulers_online, System.schedulers())
      assert partitions() == System.schedulers()
    end

    test "honours an explicit partitions setting" do
      Application.put_env(
        :logflare,
        :spool,
        Keyword.put(Application.get_env(:logflare, :spool), :partitions, 7)
      )

      assert partitions() == 7
    end
  end

  defp partitions do
    %{start: {_mod, _fun, [opts]}} = SpoolDurableBufferSup.child_spec([])
    Keyword.fetch!(opts, :partitions)
  end
end
