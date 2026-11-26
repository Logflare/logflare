defmodule Logflare.RecordUsages.SyncWorkerTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Auth
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.RecordUsages
  alias Logflare.RecordUsages.Cache
  alias Logflare.RecordUsages.RecordUsage
  alias Logflare.RecordUsages.SyncWorker
  alias Logflare.RecordUsages.Writer
  alias Logflare.User
  alias Logflare.Users

  test "periodic flush persists completed usage without waiting for pending writes" do
    token = insert(:access_token)
    key = {OauthAccessToken, token.id}
    timestamp = 1_767_225_600
    Writer.record(key, timestamp)
    assert Cache.stream() |> Enum.to_list() == [{key, timestamp}]
    writer = Process.whereis(Writer)

    on_exit(fn ->
      :sys.resume(writer)
      Cache.reset()
    end)

    :sys.suspend(writer)
    Writer.record(key, timestamp + 19)
    start_supervised!({SyncWorker, interval: 20})

    TestUtils.retry_assert(fn ->
      assert %{last_used_at: ~U[2026-01-01 00:00:00Z]} =
               RecordUsages.get(OauthAccessToken, token.id)
    end)

    stop_supervised!(SyncWorker)
    :sys.resume(writer)
    assert Cache.stream() |> Enum.to_list() == [{key, timestamp + 19}]
  end

  test "a failed local flush is retried after the worker restarts without losing usage" do
    token = insert(:access_token)
    Writer.record({OauthAccessToken, token.id}, 1_767_225_600)
    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))
    expect(RecordUsages, :persist, fn _entries -> raise "database unavailable" end)

    assert capture_log(fn ->
             start_supervised!({SyncWorker, interval: 20})

             TestUtils.retry_assert(fn ->
               assert %{last_used_at: ~U[2026-01-01 00:00:00Z]} =
                        RecordUsages.get(OauthAccessToken, token.id)

               assert Cache.stream() |> Enum.to_list() == []
             end)
           end) =~ "database unavailable"
  end

  test "flush persists token and legacy key usage without changing or evicting either resource" do
    user = insert(:user)
    token = insert(:access_token, id: user.id, resource_owner: user)
    original = Repo.reload!(token)
    original_user = Repo.reload!(user)
    timestamp = ~U[2026-01-01 00:00:00Z]

    assert {:ok, cached_token, _} = Auth.Cache.verify_access_token(token.token, [])
    cached_user = Users.Cache.get_by(api_key: user.api_key)

    refute Ecto.assoc_loaded?(cached_token.usage)
    Writer.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp, :second))
    Writer.record({User, user.id}, DateTime.to_unix(timestamp, :second) + 10)

    assert :ok = RecordUsages.flush()
    assert [%{usage: %{last_used_at: ^timestamp}}] = Auth.list_valid_access_tokens(user)

    assert Repo.get_by!(RecordUsage, legacy_api_key_user_id: user.id).last_used_at ==
             ~U[2026-01-01 00:00:10Z]

    assert Repo.reload!(token) == original
    assert Repo.reload!(user) == original_user
    assert Cache.stream() |> Enum.to_list() == []

    reject(&Auth.verify_access_token/2)
    reject(&Users.get_by/1)
    assert {:ok, ^cached_token, _} = Auth.Cache.verify_access_token(token.token, [])
    assert Users.Cache.get_by(api_key: user.api_key) == cached_user
  end

  test "flush of equal and older usage does not rewrite rows, while newer usage advances the timestamp" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    old = ~U[2025-12-31 23:59:59Z]
    latest = ~U[2026-01-01 00:00:00Z]
    newer = ~U[2026-01-01 00:00:01Z]

    Writer.record({OauthAccessToken, token.id}, DateTime.to_unix(latest, :second))
    assert :ok = RecordUsages.flush()

    row_query =
      from(usage in RecordUsage,
        where: usage.oauth_access_token_id == ^token.id,
        select: {usage.last_used_at, fragment("ctid::text")}
      )

    original_row = Repo.one!(row_query)
    assert {^latest, _version} = original_row

    for timestamp <- [latest, old] do
      Writer.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp, :second))
      assert :ok = RecordUsages.flush()
      assert Repo.one!(row_query) == original_row
      assert Cache.stream() |> Enum.to_list() == []
    end

    Writer.record({OauthAccessToken, token.id}, DateTime.to_unix(newer, :second))
    assert :ok = RecordUsages.flush()
    assert {^newer, _version} = Repo.one!(row_query)
  end

  test "flush retains newer arrivals to persisted keys for the next flush" do
    user = insert(:user)
    [first, last] = tokens = insert_list(2, :access_token, resource_owner: user)
    first_key = {OauthAccessToken, first.id}
    last_key = {OauthAccessToken, last.id}
    timestamp = 1_767_225_600
    entries = tokens |> Enum.map(&{{OauthAccessToken, &1.id}, timestamp})
    assert Cache |> Cachex.put_many!(entries)

    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))

    expect(RecordUsages, :persist, fn batch ->
      :ok = call_original(RecordUsages, :persist, [batch])
      Writer.record(first_key, timestamp + 2)
      Writer.record(last_key, timestamp + 30)
      Writer.drain()
    end)

    assert :ok = RecordUsages.flush()

    assert RecordUsages.get(OauthAccessToken, first.id).last_used_at ==
             ~U[2026-01-01 00:00:00Z]

    assert RecordUsages.get(OauthAccessToken, last.id).last_used_at ==
             ~U[2026-01-01 00:00:00Z]

    assert Map.new(Cache.stream()) == %{first_key => timestamp + 2, last_key => timestamp + 30}

    assert :ok = RecordUsages.flush()
    assert Cache.stream() |> Enum.to_list() == []

    assert RecordUsages.get(OauthAccessToken, first.id).last_used_at ==
             ~U[2026-01-01 00:00:02Z]

    assert RecordUsages.get(OauthAccessToken, last.id).last_used_at ==
             ~U[2026-01-01 00:00:30Z]
  end

  test "ignores deleted resources and cascades persisted usage when a resource is deleted" do
    for {schema, foreign_key, factory} <- [
          {OauthAccessToken, :oauth_access_token_id, :access_token},
          {User, :legacy_api_key_user_id, :user}
        ] do
      resource = insert(factory)
      deleted_resource = insert(factory)
      RecordUsages.record(schema, resource.id)
      RecordUsages.record(schema, deleted_resource.id)
      Repo.delete!(deleted_resource)

      assert :ok = RecordUsages.flush()
      assert Cache.stream() |> Enum.to_list() == []
      assert Repo.get_by(RecordUsage, [{foreign_key, deleted_resource.id}]) == nil
      assert Repo.get_by!(RecordUsage, [{foreign_key, resource.id}]).last_used_at != nil

      Repo.delete!(resource)
      assert Repo.get_by(RecordUsage, [{foreign_key, resource.id}]) == nil
    end
  end

  test "legacy usage survives key rotation" do
    user = insert(:user)
    timestamp = 1_767_225_600
    Writer.record({User, user.id}, timestamp)
    {:ok, user} = Users.update_user_all_fields(user, %{api_key: "rotated-legacy-key"})

    assert :ok = RecordUsages.flush()
    usage = Repo.get_by!(RecordUsage, legacy_api_key_user_id: user.id)
    assert usage.last_used_at == ~U[2026-01-01 00:00:00Z]
  end

  test "a later flush batch failure retains only failed and unprocessed usage" do
    user = insert(:user)
    tokens = insert_list(2_001, :access_token, resource_owner: user)
    timestamp = 1_767_225_600
    entries = tokens |> Enum.map(&{{OauthAccessToken, &1.id}, timestamp})
    assert Cache |> Cachex.put_many!(entries)
    snapshot = Cache.stream() |> Enum.sort()

    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))
    expect(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))
    expect(RecordUsages, :persist, fn _entries -> raise "database unavailable" end)

    assert_raise RuntimeError, "database unavailable", fn ->
      RecordUsages.flush()
    end

    assert Cache.stream() |> Enum.sort() == Enum.drop(snapshot, 1_000)

    persisted_ids =
      from(usage in RecordUsage,
        order_by: usage.oauth_access_token_id,
        select: usage.oauth_access_token_id
      )
      |> Repo.all()

    assert persisted_ids == tokens |> Enum.map(& &1.id) |> Enum.sort() |> Enum.take(1_000)

    assert :ok = RecordUsages.flush()
    assert Cache.stream() |> Enum.to_list() == []
    assert Repo.aggregate(RecordUsage, :count) == 2_001
  end

  test "flush with no buffered usage succeeds without persisting anything" do
    reject(&RecordUsages.persist/1)
    assert :ok = RecordUsages.flush()
    assert Cache.stream() |> Enum.to_list() == []
  end
end
