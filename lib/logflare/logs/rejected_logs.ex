defmodule Logflare.Logs.RejectedEvents do
  alias Logflare.{Source, User}
  @cache __MODULE__
  import Cachex.Spec

  def child_spec(_) do
    %{id: @cache, start: {Cachex, :start_link, [@cache, []]}}
  end
end
