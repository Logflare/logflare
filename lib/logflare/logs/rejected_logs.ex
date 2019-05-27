defmodule Logflare.Logs.RejectedEvents do
  alias Logflare.{Source, User}
  @cache __MODULE__
  import Cachex.Spec

  def child_spec(_) do
    %{id: @cache, start: {Cachex, :start_link, [@cache, []]}}
  end

  @spec get_by_user(Logflare.User.t()) :: map
  def get_by_user(%User{sources: sources}) do
    for source <- sources, into: Map.new() do
      {source.token, get_by_source(source)}
    end
  end

  @spec get_by_source(Logflare.Source.t()) :: map
  def get_by_source(%Source{token: token}) do
    get!(token)
  end

  @doc """
  Expected to be called only in a log event params validation plug
  """
  def injest(%{error: error, batch: batch, source: %Source{token: token}}) do
    log = %{
      message: error.message(),
      payload: batch
    }

    insert(token, log)
  end

  defp get!(key) do
    {:ok, val} = Cachex.get(@cache, key)
    val
  end

  @spec insert(atom, map) :: list(map)
  def insert(token, log) do
    Cachex.get_and_update!(@cache, token, fn
      xs when is_list(xs) -> Enum.take([log | xs], 500)
      _ -> [log]
    end)
  end
end
