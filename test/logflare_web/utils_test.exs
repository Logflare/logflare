defmodule LogflareWeb.UtilsTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Rules
  alias Logflare.Utils, as: LogflareUtils
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
      assert LogflareUtils.flag("some-feature") == true
      assert LogflareUtils.flag("another-feature", %Logflare.User{}) == true
    end

    @tag env: :prod
    test "returns false for unknown features when no overrides are set" do
      assert LogflareUtils.flag("unknown-feature") == false
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
      assert LogflareUtils.flag("enabled-feature") == true
      assert LogflareUtils.flag("disabled-feature") == false
      assert LogflareUtils.flag("nonexistent-feature") == false
      assert LogflareUtils.flag("truthy-feature") == true
      assert LogflareUtils.flag("falsy-feature") == false
      assert LogflareUtils.flag("other-value") == false
      assert LogflareUtils.flag("empty-string") == false
    end

    @tag env: :prod, config_cat_key: "test-sdk-key"
    test "uses ConfigCat when SDK key is present and env is not test" do
      pid = self()

      ConfigCat
      |> expect(:get_value, fn feature, default ->
        send(pid, {:get_value_called, feature, default})
        default
      end)

      assert LogflareUtils.flag("test-feature") == false

      TestUtils.retry_assert(fn ->
        assert_received {:get_value_called, "test-feature", false}
      end)

      ConfigCat
      |> expect(:get_value, fn feature, default ->
        send(pid, {:get_value_called, feature, default})
        true
      end)

      assert LogflareUtils.flag("enabled-feature") == true

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

      assert LogflareUtils.flag("test-feature", user) == true

      TestUtils.retry_assert(fn ->
        assert_received {:new_called, "test@example.com"}
      end)

      TestUtils.retry_assert(fn ->
        assert_received {:get_value_called, "test-feature", false, :user_obj}
      end)
    end
  end

  describe "stringify_changeset_errors/1" do
    test "converts single field error to string" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, sources: [source], user: user)

      assert {:error, %Ecto.Changeset{} = changeset} =
               Rules.create_rule(%{
                 source_id: source.id,
                 backend_id: backend.id,
                 lql_string: ""
               })

      assert changeset.valid? == false
      assert Utils.stringify_changeset_errors(changeset) == "lql_string: can't be blank"
    end

    test "handles errors with interpolation values" do
      changeset = %Ecto.Changeset{
        errors: [
          age: {"must be greater than %{number}", [number: 18]},
          name: {"should be at most %{count} character(s)", [count: 255]}
        ],
        data: %{},
        types: %{age: :integer, name: :string}
      }

      expected = """
      name: should be at most 255 character(s)
      age: must be greater than 18\
      """

      assert Utils.stringify_changeset_errors(changeset) == expected
    end

    test "handles multiple errors for the same field" do
      changeset = %Ecto.Changeset{
        errors: [
          email: {"can't be blank", []},
          email: {"is invalid format", []}
        ],
        data: %{},
        types: %{email: :string}
      }

      assert Utils.stringify_changeset_errors(changeset) ==
               "email: can't be blank & is invalid format"
    end
  end

  describe "stringify_changeset_errors/2" do
    test "handles multiple errors with default message" do
      changeset = %Ecto.Changeset{
        errors: [
          name: {"can't be blank", []},
          email: {"is invalid", []}
        ],
        data: %{},
        types: %{name: :string, email: :string}
      }

      assert Utils.stringify_changeset_errors(changeset, "Form submission failed") ==
               "Form submission failed: name: can't be blank; email: is invalid"
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
