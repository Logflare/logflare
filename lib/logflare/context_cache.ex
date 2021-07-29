defmodule Logflare.ContextCache do
  @moduledoc false

  def apply_fun(context, {fun, arity}, args) do
    cache = Module.concat(context, Cache)

    cache_key = {{fun, arity}, args}

    # KEY: {{:get_by_and_preload, 1}, [[api_key: "ZvQ2p6Rf-TbR"]]}
    # KEY: {{:valid_source_token_param?, 1}, ["db2d9f5c-6d8b-4024-94ad-ff9151a6236e"]}
    # KEY: {{:get_by_and_preload, 1}, [[token: "db2d9f5c-6d8b-4024-94ad-ff9151a6236e"]]}
    # KEY: {{:get_billing_account_by, 1}, [[user_id: 36]]}
    # KEY: {{:get_plan_by, 1}, [[name: "Free"]]}

    case Cachex.fetch(cache, cache_key, fn {_type, args} ->
           {:commit, apply(context, fun, args)}
         end) do
      {:commit, value} -> value
      {:ok, value} -> value
    end
  end
end
