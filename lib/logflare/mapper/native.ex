defmodule Logflare.Mapper.Native do
  @moduledoc false

  use Rustler, otp_app: :logflare, crate: "mapper_ex"

  @spec compile_mapping(map()) :: {:ok, reference()} | {:error, String.t()}
  def compile_mapping(_config), do: :erlang.nif_error(:nif_not_loaded)

  @type map_options :: boolean() | {boolean(), Logflare.Mapper.OutputContext.t() | nil}

  @spec map(term(), reference(), map_options()) ::
          map() | {:ok, binary()} | {:error, String.t()}
  def map(_document, _compiled_mapping, _options \\ false),
    do: :erlang.nif_error(:nif_not_loaded)
end
