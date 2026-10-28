defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.Config do
  @moduledoc false

  use TypedEctoSchema

  @primary_key false

  typed_embedded_schema do
    field :url, :string
    field :headers, :map
    field :labels, :map
    field :username, :string, virtual: true, redact: true
    field :password, :string, virtual: true, redact: true
  end
end
