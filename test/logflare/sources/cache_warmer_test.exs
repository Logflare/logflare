defmodule Logflare.Sources.CacheWarmerTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Billing
  alias Logflare.Sources
  alias Logflare.Sources.Cache
  alias Logflare.Sources.CacheWarmer
  alias Logflare.Sources.Source

  setup do
    plan = insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user, log_events_updated_at: NaiveDateTime.utc_now())
    Cachex.clear!(Cache)

    {:ok, expected_retention_days: Sources.source_ttl_to_days(source, plan), source: source}
  end

  test "warms ID and internal and external token entries under the read-path keys", %{
    expected_retention_days: expected_retention_days,
    source: source
  } do
    source_id = source.id
    external_token = Atom.to_string(source.token)

    assert %{id: ^source_id, retention_days: ^expected_retention_days} =
             Cache.get_by(id: source.id)

    assert %{id: ^source_id, retention_days: ^expected_retention_days} =
             Cache.get_by(token: source.token)

    assert %{id: ^source_id, retention_days: ^expected_retention_days} =
             Cache.get_by(token: external_token)

    assert {:ok, read_keys} = Cachex.keys(Cache)
    assert length(read_keys) == 3

    assert {:ok, pairs} = CacheWarmer.execute(nil)
    Cachex.clear!(Cache)
    assert {:ok, true} = Cachex.put_many(Cache, pairs)

    for read_key <- read_keys do
      assert {:ok, {:cached, %{id: ^source_id, retention_days: ^expected_retention_days}}} =
               Cachex.get(Cache, read_key)
    end
  end

  test "serves warmed ID and token entries without falling back to the database", %{
    expected_retention_days: expected_retention_days,
    source: source
  } do
    source_id = source.id
    external_token = Atom.to_string(source.token)

    assert {:ok, pairs} = CacheWarmer.execute(nil)
    assert {:ok, true} = Cachex.put_many(Cache, pairs)

    Sources
    |> reject(:get_by, 1)

    assert %{id: ^source_id, retention_days: ^expected_retention_days} =
             Cache.get_by(id: source.id)

    assert %{id: ^source_id, retention_days: ^expected_retention_days} =
             Cache.get_by(token: source.token)

    assert %{id: ^source_id, retention_days: ^expected_retention_days} =
             Cache.get_by(token: external_token)
  end

  test "hydrates retention values for sources from different user plans", %{
    expected_retention_days: expected_retention_days,
    source: source
  } do
    custom_plan =
      insert(:plan,
        name: "Custom",
        stripe_id: "custom-plan",
        limit_source_ttl: :timer.hours(24 * 7)
      )

    custom_user = insert(:user, billing_enabled: true)
    insert(:billing_account, user: custom_user, stripe_plan_id: custom_plan.stripe_id)

    custom_source =
      insert(:source,
        user: custom_user,
        log_events_updated_at: NaiveDateTime.utc_now()
      )

    assert {:ok, pairs} = CacheWarmer.execute(nil)

    warmed_sources =
      pairs
      |> Enum.map(fn {_key, {:cached, warmed_source}} -> warmed_source end)
      |> Map.new(&{&1.id, &1})

    assert %{retention_days: ^expected_retention_days} = warmed_sources[source.id]

    expected_custom_retention_days = Sources.source_ttl_to_days(custom_source, custom_plan)

    assert %{retention_days: ^expected_custom_retention_days} = warmed_sources[custom_source.id]
  end

  test "skips plan hydration when there are no active sources" do
    Repo.update_all(Source, set: [log_events_updated_at: nil])

    Billing
    |> reject(:get_plans_by_users, 1)

    assert {:ok, []} = CacheWarmer.execute(nil)
  end

  test "skips sources whose users cannot be resolved" do
    stub(Billing, :get_plans_by_users, fn _users -> %{} end)

    log =
      capture_log(fn ->
        assert {:ok, []} = CacheWarmer.execute(nil)
      end)

    assert log =~ "skipped 1 sources whose users could not be resolved"
  end

  test "uses a bounded number of queries as the number of source users grows" do
    {single_user_query_count, {:ok, _pairs}} =
      count_repo_queries(fn -> CacheWarmer.execute(nil) end)

    for _ <- 1..5 do
      user = insert(:user)
      insert(:source, user: user, log_events_updated_at: NaiveDateTime.utc_now())
    end

    {multiple_user_query_count, {:ok, _pairs}} =
      count_repo_queries(fn -> CacheWarmer.execute(nil) end)

    assert single_user_query_count > 0
    assert multiple_user_query_count == single_user_query_count
  end

  defp count_repo_queries(fun) do
    {:ok, counter} = Agent.start_link(fn -> 0 end)
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:logflare, :repo, :query],
        fn _event, _measurements, _metadata, config ->
          if self() == config.test_pid do
            Agent.update(config.counter, &(&1 + 1))
          end
        end,
        %{counter: counter, test_pid: self()}
      )

    try do
      result = fun.()
      {Agent.get(counter, & &1), result}
    after
      :telemetry.detach(handler_id)
      Agent.stop(counter)
    end
  end
end
