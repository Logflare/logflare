defmodule Logflare.Alerting.CacheTest do
  use Logflare.DataCase

  alias Logflare.Alerting
  alias Logflare.Alerting.AlertQuery
  alias Logflare.ContextCache
  alias Logflare.Repo

  setup do
    Cachex.clear!(Alerting.Cache)
    user = insert(:user)

    on_exit(fn -> Cachex.clear!(Alerting.Cache) end)

    %{user: user}
  end

  test "caches alert catalogs by user and busts them by alert id", %{user: user} do
    alert = insert(:alert, user: user, name: "original")

    assert [%AlertQuery{id: alert_id, name: "original"}] = Alerting.Cache.list_by_user_id(user.id)

    alert
    |> Ecto.Changeset.change(name: "updated")
    |> Repo.update!()

    assert [%AlertQuery{name: "original"}] = Alerting.Cache.list_by_user_id(user.id)

    assert {:ok, 1} = ContextCache.bust_keys([{Alerting, alert_id}])
    assert [%AlertQuery{name: "updated"}] = Alerting.Cache.list_by_user_id(user.id)
  end

  test "busts alert catalogs by user when an alert is inserted", %{user: user} do
    first_alert = insert(:alert, user: user)
    assert [%AlertQuery{id: first_id}] = Alerting.Cache.list_by_user_id(user.id)
    assert first_id == first_alert.id

    second_alert = insert(:alert, user: user)

    assert [%AlertQuery{id: ^first_id}] = Alerting.Cache.list_by_user_id(user.id)
    assert {:ok, 1} = ContextCache.bust_keys([{Alerting, user_id: user.id}])

    assert Alerting.Cache.list_by_user_id(user.id)
           |> Enum.map(& &1.id)
           |> Enum.sort() == Enum.sort([first_alert.id, second_alert.id])
  end

  test "does not cache empty alert catalogs", %{user: user} do
    assert [] = Alerting.Cache.list_by_user_id(user.id)
    assert Cachex.size!(Alerting.Cache) == 0

    alert = insert(:alert, user: user)

    assert [%AlertQuery{id: alert_id}] = Alerting.Cache.list_by_user_id(user.id)
    assert alert_id == alert.id
  end

  test "bypasses the alert catalog cache inside transactions", %{user: user} do
    alert = insert(:alert, user: user, name: "original")
    assert [%AlertQuery{name: "original"}] = Alerting.Cache.list_by_user_id(user.id)

    Repo.transaction(fn ->
      alert
      |> Ecto.Changeset.change(name: "updated")
      |> Repo.update!()

      assert [%AlertQuery{name: "updated"}] = Alerting.Cache.list_by_user_id(user.id)
    end)

    assert [%AlertQuery{name: "original"}] = Alerting.Cache.list_by_user_id(user.id)
  end
end
