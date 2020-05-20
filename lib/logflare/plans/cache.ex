defmodule Logflare.Plans.Cache do
  @moduledoc false
  import Cachex.Spec

  alias Logflare.Plans

  @ttl 5_000

  @cache __MODULE__

  def child_spec(_) do
    %{
      id: __MODULE__,
      start: {
        Cachex,
        :start_link,
        [
          @cache,
          [expiration: expiration(default: @ttl)]
        ]
      }
    }
  end

  def get_plan_by(keyword), do: apply_fun(__ENV__.function, [keyword])

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Plans, arg1, arg2)
  end
end
