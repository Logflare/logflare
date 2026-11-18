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
    Cache.record(key, timestamp)
    assert Cache.snapshot() == [{key, timestamp}]
    writer = Process.whereis(Writer)

    on_exit(fn ->
      :sys.resume(writer)
      Cache.reset()
    end)

    :sys.suspend(writer)
    Cache.record(key, timestamp + 19)
    start_supervised!({SyncWorker, interval: 20})

    TestUtils.retry_assert(fn ->
      assert %{last_used_at: ~U[2026-01-01 00:00:00Z]} =
               RecordUsages.get(OauthAccessToken, token.id)
    end)

    stop_supervised!(SyncWorker)
    :sys.resume(writer)
    assert Cache.snapshot() == [{key, timestamp + 19}]
  end

  test "a failed local flush is retried after the worker restarts without losing usage" do
    token = insert(:access_token)
    Cache.record({OauthAccessToken, token.id}, 1_767_225_600)
    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))
    expect(RecordUsages, :persist, fn _entries -> raise "database unavailable" end)

    assert capture_log(fn ->
             start_supervised!({SyncWorker, interval: 20})

             TestUtils.retry_assert(fn ->
               assert %{last_used_at: ~U[2026-01-01 00:00:00Z]} =
                        RecordUsages.get(OauthAccessToken, token.id)

               assert Cache.snapshot() == []
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
    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp, :second))
    Cache.record({User, user.id}, DateTime.to_unix(timestamp, :second) + 10)

    assert :ok = RecordUsages.flush()
    assert [%{usage: %{last_used_at: ^timestamp}}] = Auth.list_valid_access_tokens(user)

    assert Repo.get_by!(RecordUsage, legacy_api_key_user_id: user.id).last_used_at ==
             ~U[2026-01-01 00:00:10Z]

    assert Repo.reload!(token) == original
    assert Repo.reload!(user) == original_user
    assert Cache.snapshot() == []

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

    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(latest, :second))
    assert :ok = RecordUsages.flush()

    row_query =
      from(usage in RecordUsage,
        where: usage.oauth_access_token_id == ^token.id,
        select: {usage.last_used_at, fragment("ctid::text")}
      )

    original_row = Repo.one!(row_query)
    assert {^latest, _version} = original_row

    for timestamp <- [latest, old] do
      Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp, :second))
      assert :ok = RecordUsages.flush()
      assert Repo.one!(row_query) == original_row
      assert Cache.snapshot() == []
    end

    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(newer, :second))
    assert :ok = RecordUsages.flush()
    assert {^newer, _version} = Repo.one!(row_query)
  end

  test "flush acknowledges only its original snapshot, retaining newer arrivals" do
    user = insert(:user)
    first = insert(:access_token, resource_owner: user)
    second = insert(:access_token, resource_owner: user)
    first_key = {OauthAccessToken, first.id}
    second_key = {OauthAccessToken, second.id}
    timestamp = 1_767_225_600
    Cache.record(first_key, timestamp)
    Cache.record(second_key, timestamp + 10)

    expect(RecordUsages, :persist, fn entries ->
      result = call_original(RecordUsages, :persist, [entries])
      Cache.record(first_key, timestamp + 2)
      Cache.record(first_key, timestamp)
      assert Map.fetch!(Map.new(Cache.snapshot()), first_key) == timestamp + 2
      result
    end)

    assert :ok = RecordUsages.flush()

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: first.id).last_used_at ==
             ~U[2026-01-01 00:00:00Z]

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: second.id).last_used_at ==
             ~U[2026-01-01 00:00:10Z]

    assert Cache.snapshot() == [{first_key, timestamp + 2}]

    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))

    assert :ok = RecordUsages.flush()
    assert Cache.snapshot() == []

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: first.id).last_used_at ==
             ~U[2026-01-01 00:00:02Z]
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
      assert Cache.snapshot() == []
      assert Repo.get_by(RecordUsage, [{foreign_key, deleted_resource.id}]) == nil
      assert Repo.get_by!(RecordUsage, [{foreign_key, resource.id}]).last_used_at != nil

      Repo.delete!(resource)
      assert Repo.get_by(RecordUsage, [{foreign_key, resource.id}]) == nil
    end
  end

  test "legacy usage survives key rotation and an older buffered timestamp" do
    user = insert(:user)
    timestamp = 1_767_225_600
    Cache.record({User, user.id}, timestamp)
    {:ok, user} = Users.update_user_all_fields(user, %{api_key: "rotated-legacy-key"})

    assert :ok = RecordUsages.flush()
    usage = Repo.get_by!(RecordUsage, legacy_api_key_user_id: user.id)
    assert usage.last_used_at == ~U[2026-01-01 00:00:00Z]

    Cache.record({User, user.id}, timestamp - 10)
    assert :ok = RecordUsages.flush()
    assert Repo.get_by!(RecordUsage, legacy_api_key_user_id: user.id) == usage
  end

  test "a later flush batch failure retains all original usage for replay" do
    user = insert(:user)
    tokens = insert_list(1_001, :access_token, resource_owner: user)
    timestamp = 1_767_225_600
    Enum.each(tokens, &Cache.record({OauthAccessToken, &1.id}, timestamp))
    snapshot = Enum.sort(Cache.snapshot())

    stub(RecordUsages, :persist, fn entries ->
      if Repo.aggregate(RecordUsage, :count) == 0 do
        assert length(entries) == 1_000
        call_original(RecordUsages, :persist, [entries])
      else
        assert length(entries) == 1
        raise "second batch unavailable"
      end
    end)

    assert_raise RuntimeError, "second batch unavailable", fn ->
      RecordUsages.flush()
    end

    assert Enum.sort(Cache.snapshot()) == snapshot
    assert Repo.aggregate(RecordUsage, :count) == 1_000

    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))

    assert :ok = RecordUsages.flush()
    assert Cache.snapshot() == []
    assert Repo.aggregate(RecordUsage, :count) == 1_001
  end

  test "flush with no buffered usage succeeds without persisting anything" do
    reject(&RecordUsages.persist/1)
    assert :ok = RecordUsages.flush()
    assert Cache.snapshot() == []
  end
end
