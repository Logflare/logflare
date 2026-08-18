defmodule Logflare.Mapper do
  @moduledoc """
  Generic document mapper backed by a Rust NIF.

  Maps arbitrary Elixir maps based on configurable field definitions with
  coalesced path resolution. Configurations return output maps by default and
  can select another compiled output format. Paths use `$`-prefixed dot notation
  (e.g. `$.resource.service.name`) to navigate nested maps.

  Designed for a two-phase workflow: compile a `MappingConfig` once with
  `compile!/1`, then apply it to many documents with `map/3`. The compiled
  reference is a NIF resource that can be reused across calls without
  recompilation.

  When the input is a pre-flattened map where nested paths exist as literal
  dot-notation keys, pass `flat_keys: true` to `map/3` so that paths are
  resolved as direct key lookups instead of nested map traversal.
  """

  alias __MODULE__.MappingConfig
  alias __MODULE__.Native

  @doc "Compiles a mapping config into a NIF resource."
  @spec compile(MappingConfig.t()) :: {:ok, reference()} | {:error, String.t()}
  def compile(%MappingConfig{} = config) do
    config
    |> MappingConfig.to_nif_map()
    |> Native.compile_mapping()
  end

  @doc "Like `compile/1` but raises on invalid config."
  @spec compile!(MappingConfig.t()) :: reference()
  def compile!(%MappingConfig{} = config) do
    case compile(config) do
      {:ok, compiled} -> compiled
      {:error, reason} -> raise ArgumentError, "failed to compile mapping: #{reason}"
    end
  end

  @doc "Compiles and maps a single document in one step. Not suited for high-throughput pipelines."
  @spec run(map(), MappingConfig.t(), keyword()) ::
          {:ok, map() | binary()} | {:error, String.t()}
  def run(document, %MappingConfig{} = config, opts \\ []) when is_map(document) do
    case compile(config) do
      {:ok, compiled} -> map_result(document, compiled, opts)
      {:error, _} = error -> error
    end
  end

  @doc """
  Maps a single document using a compiled mapping.

  ## Options

    * `:flat_keys` - when `true`, dotted paths (e.g., `$.resource.service.name`)
      are resolved as literal flat-key lookups on the input map instead of nested
      map navigation. Use this when passing pre-flattened input. Defaults to
      `false`.

    * `:output_context` - per-document runtime context required by some compiled
      output formats. Build this with `Logflare.Mapper.OutputContext`; map output
      ignores it.

  The compiled mapping configuration selects the output representation. Map
  output returns a map; ClickHouse RowBinary output maps the supplied document
  and returns one encoded row binary without constructing an intermediate
  Elixir map.
  """
  @spec map(map(), reference(), keyword()) :: map() | binary()
  def map(document, compiled_mapping, opts \\ []) when is_map(document) do
    document
    |> map_result(compiled_mapping, opts)
    |> unwrap_result()
  end

  @doc "Maps a document and returns a result tuple instead of raising on output errors."
  @spec map_result(map(), reference(), keyword()) ::
          {:ok, map() | binary()} | {:error, String.t()}
  def map_result(document, compiled_mapping, opts \\ []) when is_map(document) do
    flat_keys = Keyword.get(opts, :flat_keys, false)
    output_context = Keyword.get(opts, :output_context)

    case Native.map(document, compiled_mapping, {flat_keys, output_context}) do
      {:ok, output} -> {:ok, output}
      {:error, _reason} = error -> error
      output -> {:ok, output}
    end
  end

  defp unwrap_result({:ok, output}), do: output

  defp unwrap_result({:error, reason}) do
    raise ArgumentError, "failed to produce mapping output: #{reason}"
  end
end
