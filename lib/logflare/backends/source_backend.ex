defmodule Logflare.Backends.SourcesBackend do
  @moduledoc false
  use TypedEctoSchema

  alias Logflare.Backends.Backend
  alias Logflare.Sources.Source

  typed_schema "sources_backends" do
    belongs_to(:source, Source)
    belongs_to(:backend, Backend)
  end
end
