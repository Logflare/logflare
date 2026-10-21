defmodule Logflare.SystemTest do
  @moduledoc false
  # async: false -- total_memory_bytes/0 caches its result in a global
  # :persistent_term key, and these tests manipulate the env vars that
  # decide what gets cached.
  use ExUnit.Case, async: false
  import Logflare.System

  doctest Logflare.System

  setup do
    :persistent_term.erase({Logflare.System, :total_memory_bytes})

    on_exit(fn ->
      :persistent_term.erase({Logflare.System, :total_memory_bytes})
      System.delete_env("LOGFLARE_CGROUP_MEMORY_LIMIT")
      System.delete_env("LOGFLARE_CGROUP_MEMORY_PATH")
    end)
  end

  defp cgroup_file!(contents) do
    path =
      Path.join(System.tmp_dir!(), "cgroup_memory_test_#{System.unique_integer([:positive])}")

    File.write!(path, contents)
    on_exit(fn -> File.rm(path) end)
    path
  end

  test "total_memory_bytes/0 uses :memsup when LOGFLARE_CGROUP_MEMORY_LIMIT isn't set" do
    assert total_memory_bytes() == :memsup.get_system_memory_data()[:system_total_memory]
  end

  test "total_memory_bytes/0 reads the configured cgroup path when the flag is set" do
    # Real content from a prod ingest-b pod's /sys/fs/cgroup/memory.max (32Gi).
    path = cgroup_file!("34359738368\n")
    System.put_env("LOGFLARE_CGROUP_MEMORY_LIMIT", "true")
    System.put_env("LOGFLARE_CGROUP_MEMORY_PATH", path)

    assert total_memory_bytes() == 34_359_738_368
  end

  test "total_memory_bytes/0 falls back to :memsup when the cgroup path doesn't exist" do
    System.put_env("LOGFLARE_CGROUP_MEMORY_LIMIT", "true")
    System.put_env("LOGFLARE_CGROUP_MEMORY_PATH", "/no/such/path")

    assert total_memory_bytes() == :memsup.get_system_memory_data()[:system_total_memory]
  end

  test "total_memory_bytes/0 caches the result, not re-reading the file on later calls" do
    path = cgroup_file!("34359738368\n")
    System.put_env("LOGFLARE_CGROUP_MEMORY_LIMIT", "true")
    System.put_env("LOGFLARE_CGROUP_MEMORY_PATH", path)

    assert total_memory_bytes() == 34_359_738_368

    File.rm!(path)

    assert total_memory_bytes() == 34_359_738_368
  end
end
