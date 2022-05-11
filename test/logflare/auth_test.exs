defmodule Logflare.AuthTest do
  use Logflare.DataCase
  alias Logflare.Auth.ApiKeys
  alias Logflare.Factory
  setup do
    [user: Factory.insert(:user)]
  end

  describe "api keys" do
    test "can create api key" do
      {:ok, %ApiKey{}} = ApiKeys.create_api_key(user)
    end
    test "can revoke api key", %{user: user} do
      key = api_key_fixture(user)
      :ok = ApiKeys.revoke_api_key(key)
    end

  end
  defp api_key_fixture(user) do
    {:ok, key} = ApiKeys.create_api_key(user)
    key
  end
end
