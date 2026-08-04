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

  alias Logflare.LogEvent
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
      {:ok, compiled} -> {:ok, map(document, compiled, opts)}
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

    * `:mapping_config_id` - the pre-encoded mapping configuration UUID required
      by ClickHouse RowBinary output. The value can be prepared once and reused
      across calls.

  Mapping configurations without an output format return maps. A configuration
  with ClickHouse RowBinary output accepts a `LogEvent`, maps its `body`, and
  returns one encoded row binary.
  """
  @spec map(map(), reference(), keyword()) :: map() | binary()
  def map(document, compiled_mapping, opts \\ [])

  def map(%LogEvent{} = event, compiled_mapping, opts) do
    event
    |> encode_log_event()
    |> map_document(compiled_mapping, opts)
  end

  def map(document, compiled_mapping, opts) when is_map(document) do
    map_document(document, compiled_mapping, opts)
  end

  defp map_document(document, compiled_mapping, opts) do
    flat_keys = Keyword.get(opts, :flat_keys, false)
    mapping_config_id = Keyword.get(opts, :mapping_config_id)

    document
    |> Native.map(compiled_mapping, {flat_keys, mapping_config_id})
    |> unwrap_result()
  end

  defp encode_log_event(%LogEvent{
         body: body,
         id: id,
         source_uuid: source_uuid,
         source_name: source_name,
         ingested_at: ingested_at
       })
       when is_map(body) do
    ingested_at = if ingested_at, do: DateTime.to_unix(ingested_at, :microsecond)
    source_uuid = if is_atom(source_uuid), do: Atom.to_string(source_uuid), else: source_uuid
    {body, {id, source_uuid, source_name || "", ingested_at}}
  end

  defp unwrap_result({:ok, output}), do: output

  defp unwrap_result({:error, reason}) do
    raise ArgumentError, "failed to produce mapping output: #{reason}"
  end

  defp unwrap_result(output), do: output
end
