defmodule Logflare.Logs.RejectedEvents do
  alias Logflare.{Source, User}
  @cache __MODULE__
  import Cachex.Spec

  def child_spec(_) do
    %{id: @cache, start: {Cachex, :start_link, [@cache, []]}}
  end

  def get_by_user(%User{} = user) do
    get!(user.token)
  end

  def get_by_source(%Source{} = source) do
    source.user
    |> get_by_user()
    |> Enum.find(fn {k, v} -> k === source.token end)
  end

  defp get!(key) do
    Cachex.get!(@cache, key)
  end
end
