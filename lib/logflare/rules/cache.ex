defmodule Logflare.Rules.Cache do
  @moduledoc false

  alias Logflare.Rules
  alias Logflare.Utils
  import Cachex.Spec

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start: {
        Cachex,
        :start_link,
        [
          __MODULE__,
          [
            warmers: [
              warmer(required: false, module: Rules.CacheWarmer, name: Rules.CacheWarmer)
            ],
            hooks:
              [
                if(stats, do: Utils.cache_stats()),
                Utils.cache_limit(100_000)
              ]
              |> Enum.filter(& &1),
            expiration: Utils.cache_expiration_min(60, 5)
          ]
        ]
      }
    }
  end

  def list_by_source_id(id), do: apply_repo_fun(__ENV__.function, [id])
  def list_by_backend_id(id), do: apply_repo_fun(__ENV__.function, [id])

  def bust_by(kw) do
    kw
    |> Enum.map(fn
      {:source_id, source_id} -> {:list_by_source_id, [source_id]}
      {:backend_id, backend_id} -> {:list_by_backend_id, [backend_id]}
    end)
    |> Enum.reduce(0, fn key, acc ->
      case Cachex.take(__MODULE__, key) do
        {:ok, nil} -> acc
        {:ok, _value} -> acc + 1
      end
    end)
    |> then(&{:ok, &1})
  end

  defp apply_repo_fun(fun, args) do
    Logflare.ContextCache.apply_fun(Rules, fun, args)
  end
end
