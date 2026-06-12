defmodule Logflare.Backends.QueryError do
  @moduledoc false

  require Logger

  @enforce_keys [:code, :raw_error, :backend]
  defstruct [:code, :raw_error, :backend, :description]

  @type code :: :invalid_query | :connection_error | :backend_error
  @type t :: %__MODULE__{
          code: code(),
          raw_error: term(),
          backend: module(),
          description: String.t() | nil
        }

  @spec log(t(), Keyword.t()) :: t()
  def log(%__MODULE__{} = error, metadata \\ []) when is_list(metadata) do
    Logger.error(
      "Backend query error",
      metadata
      |> Keyword.merge(
        backend: inspect(error.backend),
        error_code: error.code,
        error_string: inspect(error.raw_error)
      )
    )

    error
  end
end
