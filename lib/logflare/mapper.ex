defmodule Logflare.Mapper do
  @moduledoc """
  Generic document mapper backed by a Rust NIF.

  Maps arbitrary Elixir maps to output maps based on configurable field
  definitions with coalesced path resolution. Paths use `$`-prefixed dot
  notation (e.g. `$.resource.service.name`) to navigate nested maps.

  Designed for a two-phase workflow: compile a `MappingConfig` once with
  `compile!/1`, then apply it to many documents with `map/3`. The compiled
  reference is a NIF resource that can be reused across calls without
  recompilation.

  When the input is a pre-flattened map (e.g. `LogEvent.flattened_body`)
  where nested paths exist as literal dot-notation keys, pass
  `flat_keys: true` to `map/3` so that paths are resolved as direct key
  lookups instead of nested map traversal.
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
  @spec run(map(), MappingConfig.t()) :: {:ok, map()} | {:error, String.t()}
  def run(document, %MappingConfig{} = config) when is_map(document) do
    case compile(config) do
      {:ok, compiled} -> {:ok, map(document, compiled)}
      {:error, _} = error -> error
    end
  end

  @doc """
  Maps a single document using a compiled mapping.

  ## Options

    * `:flat_keys` - when `true`, dotted paths (e.g., `$.resource.service.name`)
      are resolved as literal flat-key lookups on the input map instead of nested
      map navigation. Use this when passing pre-flattened input such as
      `LogEvent.flattened_body`. Defaults to `false`.

  """
  @spec map(map(), reference(), keyword()) :: map()
  def map(document, compiled_mapping, opts \\ []) when is_map(document) do
    flat_keys = Keyword.get(opts, :flat_keys, false)
    Native.map(document, compiled_mapping, flat_keys)
  end
end
