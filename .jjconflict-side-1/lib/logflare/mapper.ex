defmodule Logflare.Mapper do
  @moduledoc """
  Generic document mapper backed by a Rust NIF.

  Maps arbitrary Elixir maps to flat output maps based on configurable
  field definitions with coalesced path resolution.
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

  @doc "Maps a single document using a compiled mapping."
  @spec map(map(), reference()) :: map()
  def map(document, compiled_mapping) when is_map(document) do
    Native.map(document, compiled_mapping)
  end
end
