defmodule Logflare.Sources.CacheWarmer do
  use Cachex.Warmer

  import Ecto.Query

  require Logger

  alias Logflare.Billing
  alias Logflare.Repo
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.User
  @impl true
  def execute(_state) do
    # Get sources that have been active in the last day, similar to ingesting users pattern
    sources =
      from(s in Source,
        where: s.log_events_updated_at >= ago(1, "day"),
        order_by: {:desc, s.log_events_updated_at},
        limit: 1_000
      )
      |> Repo.all()
      |> put_retention_days()

    get_kv =
      for s <- sources do
        value = {:cached, s}

        [
          {{:get_by, [[id: s.id]]}, value},
          {{:get_by, [[token: s.token]]}, value}
        ]
      end

    {:ok, List.flatten(get_kv)}
  end

  defp put_retention_days([]), do: []

  defp put_retention_days(sources) do
    user_ids = sources |> Enum.map(& &1.user_id) |> Enum.uniq()

    users =
      from(u in User,
        where: u.id in ^user_ids,
        preload: :billing_account
      )
      |> Repo.all()

    plans_by_user_id = Billing.get_plans_by_users(users)

    {hydrated_sources, skipped_count} =
      Enum.reduce(sources, {[], 0}, fn source, {hydrated_sources, skipped_count} ->
        case Map.fetch(plans_by_user_id, source.user_id) do
          {:ok, plan} ->
            {[Sources.put_retention_days(source, plan) | hydrated_sources], skipped_count}

          :error ->
            {hydrated_sources, skipped_count + 1}
        end
      end)

    if skipped_count > 0 do
      Logger.warning(
        "Sources cache warmer skipped #{skipped_count} sources whose users could not be resolved"
      )
    end

    Enum.reverse(hydrated_sources)
  end
end
