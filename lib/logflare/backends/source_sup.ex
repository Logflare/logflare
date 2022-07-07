defmodule Logflare.Backends.SourceSup do
  alias Logflare.Backends.{SourceBackend, SourceBackendRegistry}

  def via_source_backend(%SourceBackend{id: id}), do: {:via, SourceBackendRegistry, id}
end
