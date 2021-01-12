defmodule Logflare.Sources.Cache do
  @moduledoc false
  import Cachex.Spec
  use Logflare.Commons
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

  def get_bq_schema(%Source{token: token}), do: do_get_schema(token)
  def get_bq_schema(source_token) when is_atom(source_token), do: do_get_schema(source_token)

  defp do_get_schema(source_token) do
    Cachex.get!(@cache, {{:source_bq_schema, 1}, source_token})
  end

  def put_bq_schema(source_token, schema) do
    Cachex.put(@cache, {{:source_bq_schema, 1}, source_token}, schema, ttl: :timer.hours(24 * 365))
  end

  def valid_source_token_param?(arg), do: apply_repo_fun(__ENV__.function, [arg])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Sources, arg1, arg2)
  end
end
