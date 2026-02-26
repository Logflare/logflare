defmodule Logflare.UtilsTest do
  use ExUnit.Case, async: true

  doctest Logflare.EnumDeepUpdate, import: true
  doctest Logflare.Utils, import: true
  doctest Logflare.Utils.Map, import: true
end

defmodule Logflare.Utils.FlagTest do
  use ExUnit.Case, async: false
  use Mimic

  alias Logflare.ConfigCatCache
  alias Logflare.User
  alias Logflare.Utils

  setup do
    start_supervised!(ConfigCatCache)

    prev_env = Application.get_env(:logflare, :env)
    prev_sdk_key = Application.get_env(:logflare, :config_cat_sdk_key)

    Application.put_env(:logflare, :env, :prod)
    Application.put_env(:logflare, :config_cat_sdk_key, "test-sdk-key")

    on_exit(fn ->
      Application.put_env(:logflare, :env, prev_env)

      if prev_sdk_key,
        do: Application.put_env(:logflare, :config_cat_sdk_key, prev_sdk_key),
        else: Application.delete_env(:logflare, :config_cat_sdk_key)
    end)

    :ok
  end

  describe "flag/2 with binary identifier (cached)" do
    test "caches ConfigCat result on first call, serves from cache on second" do
      expect(ConfigCat, :get_value, 1, fn _feature, _default, _user -> true end)

      assert Utils.flag("key_values", "id-1") == true
      # Second call hits cache, not ConfigCat (expect count = 1)
      assert Utils.flag("key_values", "id-1") == true
    end

    test "different identifiers that hash differently get separate cache entries" do
      expect(ConfigCat, :get_value, 2, fn _feature, _default, _user -> true end)

      assert Utils.flag("key_values", "id-1") == true
      assert Utils.flag("key_values", "id-2") == true
    end
  end

  describe "flag/2 with User (uncached)" do
    test "calls ConfigCat every time" do
      expect(ConfigCat, :get_value, 3, fn _feature, _default, _user -> true end)

      user = %User{email: "test@example.com"}
      assert Utils.flag("some_feature", user) == true
      assert Utils.flag("some_feature", user) == true
      assert Utils.flag("some_feature", user) == true
    end
  end

  describe "flag/2 with nil identifier" do
    test "calls ConfigCat without user object" do
      expect(ConfigCat, :get_value, 1, fn _feature, _default -> false end)

      assert Utils.flag("some_feature") == false
    end
  end

  describe "flag/2 in test env" do
    setup do
      prev_env = Application.get_env(:logflare, :env)
      Application.put_env(:logflare, :env, :test)
      on_exit(fn -> Application.put_env(:logflare, :env, prev_env) end)
      :ok
    end

    test "always returns true regardless of identifier" do
      assert Utils.flag("any_feature") == true
      assert Utils.flag("any_feature", "some-id") == true
    end
  end

  describe "flag/2 with overrides" do
    setup do
      prev_sdk_key = Application.get_env(:logflare, :config_cat_sdk_key)
      prev_override = Application.get_env(:logflare, :feature_flag_override)

      Application.delete_env(:logflare, :config_cat_sdk_key)

      Application.put_env(:logflare, :feature_flag_override, %{
        "enabled" => "true",
        "disabled" => "false"
      })

      on_exit(fn ->
        if prev_sdk_key,
          do: Application.put_env(:logflare, :config_cat_sdk_key, prev_sdk_key),
          else: Application.delete_env(:logflare, :config_cat_sdk_key)

        if prev_override,
          do: Application.put_env(:logflare, :feature_flag_override, prev_override),
          else: Application.delete_env(:logflare, :feature_flag_override)
      end)

      :ok
    end

    test "returns override value, defaulting to false" do
      assert Utils.flag("enabled") == true
      assert Utils.flag("disabled") == false
      assert Utils.flag("unknown") == false
    end
  end
end

defmodule Logflare.UtilsSyncTest do
  use ExUnit.Case, async: false

  @auth_headers [{"authorization", "some token"}, {"x-api-key", "some token"}]

  describe "Tesla.Env stringification in test env" do
    test "does not redact headers" do
      for {header, value} <- @auth_headers do
        env = %Tesla.Env{headers: [{header, value}]}
        refute Logflare.Utils.stringify(env) =~ "REDACTED"
        refute inspect(env) =~ "REDACTED"
      end
    end
  end

  describe "Tesla stringification in prod env" do
    setup do
      prev_env = Application.get_env(:logflare, :env)
      Application.put_env(:logflare, :env, :prod)
      on_exit(fn -> Application.put_env(:logflare, :env, prev_env) end)
      :ok
    end

    test "redacts sensitive headers in Tesla.Env" do
      for {header, value} <- @auth_headers do
        env = %Tesla.Env{headers: [{header, value}]}
        assert Logflare.Utils.stringify(env) =~ "REDACTED"
        assert inspect(env) =~ "REDACTED"
      end
    end

    test "redacts sensitive headers in Tesla.Env opts" do
      for {header, value} <- @auth_headers do
        env = %Tesla.Env{opts: [req_headers: [{header, value}]]}
        assert Logflare.Utils.stringify(env) =~ "REDACTED"
        assert inspect(env) =~ "REDACTED"
      end
    end

    test "redacts sensitive headers in Tesla.Client" do
      for {header, value} <- @auth_headers do
        client = %Tesla.Client{
          pre: [{Tesla.Middleware.Headers, [{header, value}]}]
        }

        assert Logflare.Utils.stringify(client) =~ "REDACTED"
        assert inspect(client) =~ "REDACTED"
      end
    end
  end
end
