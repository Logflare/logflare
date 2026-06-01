defmodule Logflare.Backends.QueryError do
  @moduledoc false

  @derive {Jason.Encoder, only: [:message]}
  @enforce_keys [:message, :code, :raw_error, :backend]
  defstruct [:message, :code, :raw_error, :backend, :description]

  @type backend ::
          Logflare.Backends.Adaptor.BigQueryAdaptor
          | Logflare.Backends.Adaptor.ClickHouseAdaptor
          | Logflare.Backends.Adaptor.PostgresAdaptor
  @type code :: :invalid_query | :connection_error | :backend_error
  @type t :: %__MODULE__{
          message: String.t(),
          code: code(),
          raw_error: term(),
          backend: backend(),
          description: String.t() | nil
        }
end
