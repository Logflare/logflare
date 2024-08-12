defmodule Logflare.Backends.SourcesBackend do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Source
  alias Logflare.User
  alias Logflare.Rule

  typed_schema "sources_backends" do
    belongs_to(:source, Source)
    belongs_to(:backend, Backend)
  end
end
