defmodule Logflare.Mapper.Native do
  @moduledoc false

  use Rustler, otp_app: :logflare, crate: "mapper_ex"

  @spec compile_mapping(map()) :: {:ok, reference()} | {:error, String.t()}
  def compile_mapping(_config), do: :erlang.nif_error(:nif_not_loaded)

  @spec map(map(), reference(), boolean()) :: map()
  def map(_body, _compiled_mapping, _flat_keys \\ false), do: :erlang.nif_error(:nif_not_loaded)

  @spec map_and_encode_clickhouse(
          {map(), {binary(), binary(), binary(), integer() | nil}},
          reference(),
          :log | :metric | :trace,
          binary()
        ) :: {:ok, binary()} | {:error, String.t()}
  def map_and_encode_clickhouse(
        _document,
        _compiled_mapping,
        _event_type,
        _mapping_config_id
      ),
      do: :erlang.nif_error(:nif_not_loaded)
end
