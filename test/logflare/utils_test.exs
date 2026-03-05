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
  use ExUnitProperties

  alias Logflare.Backends.Backend
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.OauthAccessTokens.PartnerOauthAccessToken
  alias Logflare.User
  import ExUnit.CaptureLog
  require Logger

  @auth_headers [{"authorization", "some token"}, {"x-api-key", "some token"}]

  describe "Tesla.Env stringification in test env" do
    test "does not redact headers" do
      for {header, value} <- @auth_headers do
        env = %Tesla.Env{headers: [{header, value}]}
        refute Logflare.Utils.stringify(env) =~ "REDACTED"
        refute inspect(env) =~ "REDACTED"
      end
    end

    test "does not nilify backend config fields" do
      backend = %Backend{
        config: %{token: "some token"},
        config_encrypted: %{token: "some token"}
      }

      assert Logflare.Utils.stringify(backend) =~ "some token"
      assert inspect(backend) =~ "some token"
      refute Logflare.Utils.stringify(backend) =~ "config: nil"
      refute inspect(backend) =~ "config: nil"
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
        refute Logflare.Utils.stringify(env) =~ "some token"
        assert inspect(env) =~ "REDACTED"
        refute inspect(env) =~ "some token"
      end
    end

    test "redacts sensitive headers in Tesla.Env opts" do
      for {header, value} <- @auth_headers do
        env = %Tesla.Env{opts: [req_headers: [{header, value}]]}
        assert Logflare.Utils.stringify(env) =~ "REDACTED"
        refute Logflare.Utils.stringify(env) =~ "some token"
        assert inspect(env) =~ "REDACTED"
        refute inspect(env) =~ "some token"
      end
    end

    test "redacts sensitive headers in Tesla.Client" do
      for {header, value} <- @auth_headers do
        client = %Tesla.Client{
          pre: [{Tesla.Middleware.Headers, :call, [[{header, value}]]}]
        }

        assert inspect(client) =~ "REDACTED"
        refute inspect(client) =~ "some token"
      end
    end

    test "nilifies user api_key fields" do
      user = %User{api_key: "some token", old_api_key: "old token"}

      assert Logflare.Utils.stringify(user) =~ "api_key: nil"
      assert Logflare.Utils.stringify(user) =~ "old_api_key: nil"
      refute Logflare.Utils.stringify(user) =~ "some token"
      refute Logflare.Utils.stringify(user) =~ "old token"
      assert inspect(user) =~ "api_key: nil"
      assert inspect(user) =~ "old_api_key: nil"
      refute inspect(user) =~ "some token"
      refute inspect(user) =~ "old token"

      log =
        capture_log(fn ->
          try do
            raise RuntimeError, message: inspect(user)
          rescue
            error ->
              Logger.error(error)
          end
        end)

      refute log =~ "some token"
      refute log =~ "old token"
      assert log =~ "api_key: nil"
      assert log =~ "old_api_key: nil"
    end

    test "nilifies oauth access token token field" do
      oauth_access_token = %OauthAccessToken{token: "some token"}

      assert Logflare.Utils.stringify(oauth_access_token) =~ "token: nil"
      refute Logflare.Utils.stringify(oauth_access_token) =~ "some token"
      assert inspect(oauth_access_token) =~ "token: nil"
      refute inspect(oauth_access_token) =~ "some token"

      log =
        capture_log(fn ->
          try do
            raise RuntimeError, message: inspect(oauth_access_token)
          rescue
            error ->
              Logger.error(error)
          end
        end)

      refute log =~ "some token"
      assert log =~ "token: nil"
    end

    test "nilifies partner oauth access token token field" do
      partner_oauth_access_token = %PartnerOauthAccessToken{token: "some token"}

      assert Logflare.Utils.stringify(partner_oauth_access_token) =~ "token: nil"
      refute Logflare.Utils.stringify(partner_oauth_access_token) =~ "some token"
      assert inspect(partner_oauth_access_token) =~ "token: nil"
      refute inspect(partner_oauth_access_token) =~ "some token"

      log =
        capture_log(fn ->
          try do
            raise RuntimeError, message: inspect(partner_oauth_access_token)
          rescue
            error ->
              Logger.error(error)
          end
        end)

      refute log =~ "some token"
      assert log =~ "token: nil"
    end

    test "nilifies backend config fields" do
      backend = %Backend{
        config: %{token: "some token"},
        config_encrypted: %{token: "some token"}
      }

      assert Logflare.Utils.stringify(backend) =~ "config: nil"
      assert Logflare.Utils.stringify(backend) =~ "config_encrypted: nil"
      refute Logflare.Utils.stringify(backend) =~ "some token"
      assert inspect(backend) =~ "config: nil"
      assert inspect(backend) =~ "config_encrypted: nil"
      refute inspect(backend) =~ "some token"

      log =
        capture_log(fn ->
          try do
            raise RuntimeError, message: inspect(backend)
          rescue
            error ->
              Logger.error(error)
          end
        end)

      refute log =~ "some token"
      assert log =~ "config: nil"
      assert log =~ "config_encrypted: nil"
    end

    test "redacts sensitive headers in Task crash with MatchError" do
      client = %Tesla.Client{
        pre: [{Tesla.Middleware.Headers, :call, [[{"authorization", "secret_token_123"}]]}]
      }

      log =
        capture_log(fn ->
          {:ok, pid} =
            Task.start(fn ->
              data =
                {:error,
                 %Tesla.Env{
                   headers: [{"x-request-id", "abc"}, {"x-api-key", "secret_token_123"}],
                   __client__: client
                 }}

              {:ok, _} = Function.identity(data)
            end)

          ref = Process.monitor(pid)
          assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1000
        end)

      assert log =~ "MatchError"
      assert log =~ "REDACTED"
      refute log =~ "secret_token_123"
    end

    test "redacts sensitive headers in Task crash with error tuple termination" do
      log =
        capture_log(fn ->
          {:ok, pid} =
            Task.start(fn ->
              GenServer.call(
                self(),
                {:error,
                 %Tesla.Env{
                   headers: [
                     {"content-type", "application/json"},
                     {"authorization", "Bearer secret_bearer_token_789"}
                   ]
                 }}
              )
            end)

          ref = Process.monitor(pid)
          assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1000
          Process.sleep(500)
        end)

      refute log =~ "secret_bearer_token_789"
      assert log =~ "REDACTED"
    end

    test "redacts sensitive user fields in Task crash with Phoenix.Template.UndefinedError" do
      log =
        capture_log(fn ->
          {:ok, pid} =
            Task.start(fn ->
              raise %Phoenix.Template.UndefinedError{
                module: LogflareWeb.PhoenixOauth2Provider.AuthorizedApplicationView,
                template: "index.html",
                assigns: %{
                  user: %User{api_key: "secret_api_key_123", old_api_key: "old_secret_key_456"}
                }
              }
            end)

          ref = Process.monitor(pid)
          assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1000
          Process.sleep(500)
        end)

      assert log =~ "UndefinedError"
      refute log =~ "secret_api_key_123"
      refute log =~ "old_secret_key_456"
    end
  end

  describe "redaction property tests" do
    setup do
      prev_env = Application.get_env(:logflare, :env)
      Application.put_env(:logflare, :env, :prod)
      on_exit(fn -> Application.put_env(:logflare, :env, prev_env) end)
      :ok
    end

    @sensitive_headers ["authorization", "x-api-key", "Authorization", "X-API-Key"]
    @secret "super_secret_token_value_42"

    defp redactable_input do
      safe_header =
        bind(string(:alphanumeric, min_length: 5, max_length: 20), fn name ->
          bind(string(:alphanumeric, min_length: 1, max_length: 20), fn value ->
            constant({name, value})
          end)
        end)

      headers_gen =
        bind(member_of(@sensitive_headers), fn header ->
          bind(list_of(safe_header, max_length: 5), fn safe_headers ->
            constant(Enum.shuffle(safe_headers ++ [{header, @secret}]))
          end)
        end)

      random_values = one_of([constant(nil), string(:alphanumeric, min_length: 3), integer()])

      struct_gen =
        one_of([
          bind(headers_gen, fn headers ->
            constant(%Tesla.Env{headers: headers})
          end),
          bind(headers_gen, fn headers ->
            bind(keyword_of(random_values), fn opts ->
              constant(%Tesla.Env{opts: opts ++ [req_headers: headers], headers: headers})
            end)
          end),
          bind(string(:alphanumeric, min_length: 3), fn email ->
            constant(%User{api_key: @secret, old_api_key: @secret, email: email})
          end),
          bind(positive_integer(), fn id ->
            constant(%OauthAccessToken{token: @secret, resource_owner_id: id})
          end),
          bind(positive_integer(), fn id ->
            constant(%PartnerOauthAccessToken{token: @secret, resource_owner_id: id})
          end),
          constant(%Backend{config: %{token: @secret}, config_encrypted: %{token: @secret}})
        ])

      exception_gen =
        bind(struct_gen, fn struct ->
          one_of([
            constant(%ErlangError{original: struct}),
            constant(%MatchError{term: struct}),
            constant(%MatchError{term: {:some_error, struct}}),
            constant(%Phoenix.Template.UndefinedError{
              module: LogflareWeb.SomeView,
              template: "show.html",
              assigns: %{user: struct}
            }),
            constant(%Phoenix.Template.UndefinedError{
              module: LogflareWeb.SomeView,
              template: "index.html",
              assigns: %{data: struct}
            }),
            bind(string(:alphanumeric, min_length: 3), fn msg ->
              constant(%RuntimeError{message: "#{msg} #{inspect(struct)} #{msg}"})
            end),
            bind(string(:alphanumeric, min_length: 3), fn reason ->
              constant({:stop, reason, %{state: struct}})
            end),
            bind(string(:alphanumeric, min_length: 3), fn reason ->
              constant({:noproc, {GenServer, :call, [struct, reason]}})
            end)
          ])
        end)

      random_redactables =
        one_of([
          struct_gen,
          random_values,
          exception_gen
        ])

      one_of([
        exception_gen,
        keyword_of(one_of([struct_gen, exception_gen])),
        map_of(string(:alphanumeric), one_of([struct_gen, exception_gen])),
        bind(list_of(struct_gen), fn list -> constant(MapSet.new(list)) end),
        list_of(struct_gen),
        list_of(map_of(string(:alphanumeric), struct_gen)),
        bind(list_of(random_redactables), fn list -> constant(List.to_tuple(list)) end)
      ])
    end

    property "redacts sensitive values across all struct types and positions" do
      check all to_redact <- redactable_input() do
        refute Logflare.Utils.stringify(to_redact) =~ @secret
        refute inspect(to_redact) =~ @secret
      end
    end
  end
end
