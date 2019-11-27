defmodule Logflare.Sources.Buffers do
  @moduledoc false

  @cache __MODULE__

  require Logger

  def child_spec(_) do
    cachex_opts = []

    %{
      id: :cachex_buffers_state,
      start: {Cachex, :start_link, [@cache, cachex_opts]}
    }
  end

  def put_buffer_len(source_id, buffer) when is_atom(source_id) do
    len = :queue.len(buffer)
    Cachex.put(@cache, source_id, len)
  end

  def dirty_len(source_id) when is_atom(source_id) do
    case Cachex.get(@cache, source_id) do
      {:ok, nil} ->
        0

      {:ok, len} ->
        len

      {:error, _} ->
        0
    end
  end
end
