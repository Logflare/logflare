defmodule Logflare.Backends.Adaptor.QueryResult do
  @moduledoc false

  @enforce_keys [:rows]
  defstruct rows: [], meta: %{}

  @type t :: t(map())
  @type t(meta) :: %__MODULE__{
          rows: [term()],
          meta: meta
        }

  @spec new([term()], map()) :: t()
  def new(rows, meta \\ %{}) when is_list(rows) and is_map(meta) do
    %__MODULE__{rows: rows, meta: meta}
  end

  @spec meta(t(), atom(), any()) :: any()
  def meta(%__MODULE__{meta: meta}, key, default \\ nil) when is_atom(key) do
    Map.get(meta, key, default)
  end

  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{rows: rows, meta: meta}) when is_map(meta) do
    Map.put(meta, :rows, rows)
  end
end
