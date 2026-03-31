defmodule Logflare.Backends.Adaptor.QueryResult do
  @moduledoc false

  @not_supported :not_supported
  @typep not_supported :: :not_supported

  @enforce_keys [:rows, :total_rows]
  defstruct [
    :rows,
    :total_rows,
    total_bytes_processed: @not_supported,
    query_string: @not_supported,
    bq_params: @not_supported
  ]

  @type t :: %__MODULE__{
          rows: [term()],
          total_bytes_processed: integer() | not_supported(),
          total_rows: integer(),
          query_string: String.t() | not_supported(),
          bq_params: [map()] | not_supported()
        }

  @spec new([term()], map()) :: t()
  def new(rows, attrs \\ %{}) when is_list(rows) do
    attrs =
      attrs
      |> Map.put(:rows, rows)
      |> Map.put_new(:total_rows, length(rows))

    struct!(__MODULE__, attrs)
  end
end
