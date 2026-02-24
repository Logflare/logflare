defmodule Logflare.Mapper.Native do
  @moduledoc false

  use Rustler, otp_app: :logflare, crate: "mapper_ex"

  @spec compile_mapping(map()) :: {:ok, reference()} | {:error, String.t()}
  def compile_mapping(_config), do: :erlang.nif_error(:nif_not_loaded)

  @spec map(map(), reference()) :: map()
  def map(_body, _compiled_mapping), do: :erlang.nif_error(:nif_not_loaded)
end
