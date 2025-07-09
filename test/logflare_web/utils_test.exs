defmodule LogflareWeb.UtilsTest do
  use LogflareWeb.ConnCase, async: false

  alias LogflareWeb.Utils

  doctest LogflareWeb.Utils, import: true

  describe "flag/2" do
    setup tags do
      original_env = Application.get_env(:logflare, :env)
      original_config_cat_key = Application.get_env(:logflare, :config_cat_sdk_key)
      original_overrides = Application.get_env(:logflare, :feature_flag_override)

      Application.put_env(:logflare, :env, tags[:env])
      Application.put_env(:logflare, :config_cat_sdk_key, tags[:config_cat_key])
      put_env_or_delete_otherwise(:logflare, :feature_flag_override, tags[:feature_overrides])

      on_exit(fn ->
        Application.put_env(:logflare, :env, original_env)
        put_env_or_delete_otherwise(:logflare, :config_cat_sdk_key, original_config_cat_key)
        put_env_or_delete_otherwise(:logflare, :feature_flag_override, original_overrides)
      end)

      :ok
    end

    @tag env: :test
    test "returns true in test environment" do
      assert Utils.flag("some-feature") == true
      assert Utils.flag("another-feature", %Logflare.User{}) == true
    end

    @tag env: :prod
    test "returns false for unknown features when no overrides are set" do
      assert Utils.flag("unknown-feature") == false
    end

    @tag env: :prod,
         feature_overrides: %{
           "enabled-feature" => "true",
           "disabled-feature" => "false",
           "truthy-feature" => "true",
           "falsy-feature" => "false",
           "other-value" => "maybe",
           "empty-string" => ""
         }
    test "handles different override string values correctly when no SDK key is present" do
      assert Utils.flag("enabled-feature") == true
      assert Utils.flag("disabled-feature") == false
      assert Utils.flag("nonexistent-feature") == false
      assert Utils.flag("truthy-feature") == true
      assert Utils.flag("falsy-feature") == false
      assert Utils.flag("other-value") == false
      assert Utils.flag("empty-string") == false
    end

    @tag env: :prod, config_cat_key: "test-sdk-key"
    test "uses ConfigCat when SDK key is present and env is not test" do
      pid = self()

      ConfigCat
      |> expect(:get_value, fn feature, default ->
        send(pid, {:get_value_called, feature, default})
        default
      end)

      assert Utils.flag("test-feature") == false

      TestUtils.retry_assert(fn ->
        assert_received {:get_value_called, "test-feature", false}
      end)

      ConfigCat
      |> expect(:get_value, fn feature, default ->
        send(pid, {:get_value_called, feature, default})
        true
      end)

      assert Utils.flag("enabled-feature") == true

      TestUtils.retry_assert(fn ->
        assert_received {:get_value_called, "enabled-feature", false}
      end)
    end

    @tag env: :prod, config_cat_key: "test-sdk-key"
    test "uses ConfigCat with user object when SDK key is present and user is provided" do
      user = build(:user, email: "test@example.com")

      pid = self()

      ConfigCat.User
      |> expect(:new, fn email ->
        send(pid, {:new_called, email})
        :user_obj
      end)

      ConfigCat
      |> expect(:get_value, fn feature, default, user ->
        send(pid, {:get_value_called, feature, default, user})
        true
      end)

      assert Utils.flag("test-feature", user) == true

      TestUtils.retry_assert(fn ->
        assert_received {:new_called, "test@example.com"}
      end)

      TestUtils.retry_assert(fn ->
        assert_received {:get_value_called, "test-feature", false, :user_obj}
      end)
    end
  end

  def put_env_or_delete_otherwise(scope, key, value) do
    if value do
      Application.put_env(scope, key, value)
    else
      Application.delete_env(scope, key)
    end
  end
end
