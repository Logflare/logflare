defmodule Logflare.ContextCache.NegativeLookupTest do
  use Logflare.DataCase

  alias Cainophile.Changes.NewRecord
  alias Cainophile.Changes.Transaction
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.Endpoints
  alias Logflare.Users

  setup do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    :ok
  end

  test "user creation invalidates a previously cached missing user" do
    api_key = build(:user).api_key
    assert nil == Users.Cache.get_by(api_key: api_key)
    user = insert(:user, api_key: api_key)
    assert nil == Users.Cache.get_by(api_key: api_key)

    dispatch(%NewRecord{
      relation: {"public", "users"},
      record: %{"id" => to_string(user.id)}
    })

    assert %{id: id} = Users.Cache.get_by(api_key: api_key)
    assert id == user.id
  end

  test "endpoint creation invalidates a previously cached missing endpoint" do
    user = insert(:user)
    token = Ecto.UUID.generate()
    assert nil == Endpoints.Cache.get_by(token: token)
    endpoint = insert(:endpoint, user: user, token: token)
    assert nil == Endpoints.Cache.get_by(token: token)

    dispatch(%NewRecord{
      relation: {"public", "endpoint_queries"},
      record: %{"id" => to_string(endpoint.id), "user_id" => to_string(user.id)}
    })

    assert %{id: id} = Endpoints.Cache.get_by(token: token)
    assert id == endpoint.id
  end

  test "not-found invalidation removes only cached nil values in its context" do
    nil_key = {:get_by, [[name: "missing"]]}
    positive_key = {:get_by, [[name: "present"]]}
    list_key = {:list_endpoints_by, [[user_id: 1]]}
    empty_key = {:catalog, {:list_endpoints_by, [[user_id: 2]]}, make_ref()}

    Cachex.put!(Endpoints.Cache, nil_key, {:cached, nil})
    Cachex.put!(Endpoints.Cache, positive_key, {:cached, %{id: 1}})
    Cachex.put!(Endpoints.Cache, list_key, {:cached, [%{id: 1}]})
    Cachex.put!(Endpoints.Cache, empty_key, {:cached, []})
    Cachex.put!(Users.Cache, nil_key, {:cached, nil})

    assert {:ok, 1} = ContextCache.bust_keys([{Endpoints, :not_found}])
    assert {:ok, false} = Cachex.exists?(Endpoints.Cache, nil_key)
    assert {:cached, %{id: 1}} = Cachex.get!(Endpoints.Cache, positive_key)
    assert {:cached, [%{id: 1}]} = Cachex.get!(Endpoints.Cache, list_key)
    assert {:cached, []} = Cachex.get!(Endpoints.Cache, empty_key)
    assert {:cached, nil} = Cachex.get!(Users.Cache, nil_key)
  end

  defp dispatch(change) do
    test_pid = self()

    expect(ContextCache, :bust_keys, fn records ->
      result = Mimic.call_original(ContextCache, :bust_keys, [records])
      send(test_pid, :busted)
      result
    end)

    send(CacheBuster, %Transaction{changes: [change]})
    assert_receive :busted, 1_000
  end
end
