defmodule Logflare.Logs.RejectedEvents do
  alias Logflare.{Source, User}
  @cache __MODULE__
  import Cachex.Spec

  def child_spec(_) do
    %{id: @cache, start: {Cachex, :start_link, [@cache, []]}}
  end

  @spec get_by_user(Logflare.User.t()) :: {atom(), any()}
  def get_by_user(%User{token: token} = user) do
    get!(token)
  end

  @spec get_by_source(Logflare.Source.t()) :: map
  def get_by_source(%Source{user: %User{} = user} = source) do
    user
    |> get_by_user()
    |> Enum.find(fn {k, _} -> k === source.token end)
  end

  @doc """
  Expected to be called only in a log event params validation plug
  """
  def injest(%{
        reason: reason,
        log_events: log_events,
        source: source
      }) do
    insert(source, reason, log_events)
  end

  defp get!(key) do
    {:ok, val} = Cachex.get(@cache, key)
    val || %{}
  end

  def insert(%Source{token: token, user: user} = source, error, value) do
    %{^token => cached} =
      Cachex.get_and_update!(@cache, source.user.id, fn
        %{^token => logs} = val -> %{val | token => Enum.take([value | logs], 100)}
        map -> Map.put(map || %{}, token, value)
      end)

    cached
  end
end
