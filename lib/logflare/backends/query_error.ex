defmodule Logflare.Backends.QueryError do
  @moduledoc false

  require Logger

  @enforce_keys [:kind, :raw_error, :backend]
  defstruct [:kind, :raw_error, :backend, :description]

  @type kind :: :invalid_query | :connection_error | :backend_error
  @type t :: %__MODULE__{
          kind: kind(),
          raw_error: term(),
          backend: module(),
          description: String.t() | nil
        }

  @spec log(t(), Keyword.t()) :: t()
  def log(error, metadata \\ [])

  def log(%__MODULE__{kind: :invalid_query} = error, metadata) when is_list(metadata) do
    error
  end

  def log(%__MODULE__{} = error, metadata) when is_list(metadata) do
    Logger.error(
      "Backend query error",
      metadata
      |> Keyword.merge(
        backend: inspect(error.backend),
        error_kind: error.kind,
        error_string: inspect(error.raw_error)
      )
    )

    error
  end
end
