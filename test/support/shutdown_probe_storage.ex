defmodule Logflare.Test.ShutdownProbeStorage do
  def put(path, _key, body, _opts) do
    File.write!(path, body)
    {:ok, %{}}
  end
end
