defmodule LogflareWeb.UtilsTest do
  use LogflareWeb.ConnCase, async: false

  alias Ecto.Changeset
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

  describe "sql_params_to_sql/2" do
    test "escapes single quotes in string parameters to produce valid SQL literals" do
      sql = "SELECT * FROM t WHERE t.col = ?"

      param = %{
        parameterType: %{type: "STRING"},
        parameterValue: %{value: "it's a trap'; DROP TABLE users --"}
      }

      result = Utils.sql_params_to_sql(sql, [param])
      assert result == "SELECT * FROM t WHERE t.col = 'it''s a trap''; DROP TABLE users --'"
      assert result =~ "''"
    end

    test "handles string values without special characters unchanged" do
      sql = "SELECT * FROM t WHERE t.col = ?"

      param = %{
        parameterType: %{type: "STRING"},
        parameterValue: %{value: "simple_value"}
      }

      assert Utils.sql_params_to_sql(sql, [param]) ==
               "SELECT * FROM t WHERE t.col = 'simple_value'"
    end

    test "renders BOOL parameters as unquoted literals" do
      sql = "SELECT * FROM t WHERE t.col = ?"

      for value <- [false, "false"] do
        param = %{parameterType: %{type: "BOOL"}, parameterValue: %{value: value}}

        assert Utils.sql_params_to_sql(sql, [param]) ==
                 "SELECT * FROM t WHERE t.col = false"
      end

      for value <- [true, "true"] do
        param = %{parameterType: %{type: "BOOL"}, parameterValue: %{value: value}}

        assert Utils.sql_params_to_sql(sql, [param]) ==
                 "SELECT * FROM t WHERE t.col = true"
      end
    end

    test "renders numeric parameters as unquoted literals" do
      sql = "SELECT * FROM t WHERE a = ? AND b = ?"

      params = [
        %{parameterType: %{type: "INTEGER"}, parameterValue: %{value: 42}},
        %{parameterType: %{type: "FLOAT"}, parameterValue: %{value: 3.5}}
      ]

      assert Utils.sql_params_to_sql(sql, params) ==
               "SELECT * FROM t WHERE a = 42 AND b = 3.5"
    end
  end

  describe "replace_table_with_source_name/2" do
    test "escapes backticks in source names to keep identifier quoting intact" do
      table_id = "`myproject`.`mydataset`.`mytable`"
      sql = "SELECT * FROM #{table_id}"

      source = %{bq_table_id: table_id, name: "evil`injection"}

      result = Utils.replace_table_with_source_name(sql, source)
      assert result == "SELECT * FROM `evil\\`injection`"
    end

    test "handles normal source names without backticks" do
      table_id = "`myproject`.`mydataset`.`mytable`"
      sql = "SELECT * FROM #{table_id}"

      source = %{bq_table_id: table_id, name: "My Log Source"}
      result = Utils.replace_table_with_source_name(sql, source)
      assert result == "SELECT * FROM `My Log Source`"
    end
  end

  describe "changeset_errors/1" do
    test "interpolates validation messages without stringifying unrelated metadata" do
      changeset =
        example_config_changeset(%{
          api_key: "",
          database: "system",
          endpoint: "clickhouse.internal:8123",
          event_types: ["log", "profile"],
          max_message_bytes: 0,
          name: "x!",
          pool_size: 101,
          port: 0,
          query_timeout: 50,
          region: "moon-1",
          retention_days: 366,
          source_token: "short"
        })

      assert Utils.changeset_errors(changeset) == %{
               api_key: ["can't be blank"],
               database: ["is reserved"],
               endpoint: ["has invalid format"],
               event_types: ["has an invalid entry"],
               max_message_bytes: ["must be greater than 0"],
               name: ["has invalid format", "should be at least 3 character(s)"],
               pool_size: ["must be less than or equal to 100"],
               port: ["is invalid"],
               query_timeout: ["must be greater than or equal to 100"],
               region: ["is invalid"],
               retention_days: ["must be less than or equal to 365"],
               source_token: ["should be 36 character(s)"]
             }
    end

    test "returns no errors for a valid backend configuration" do
      assert example_config_changeset(%{}) |> Utils.changeset_errors() == %{}
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

    test "joins multiple fields with newlines and errors with ampersands" do
      changeset = example_config_changeset(%{name: "x!", port: 0, query_timeout: 50})

      assert Utils.stringify_changeset_errors(changeset) == """
             name: has invalid format & should be at least 3 character(s)
             port: is invalid
             query_timeout: must be greater than or equal to 100\
             """
    end
  end

  describe "stringify_changeset_errors/2" do
    test "joins multiple fields with semicolons and errors with commas" do
      changeset =
        example_config_changeset(%{
          endpoint: "clickhouse.internal:8123",
          name: "x!",
          port: 0
        })

      assert Utils.stringify_changeset_errors(changeset, "Backend configuration is invalid") ==
               "Backend configuration is invalid: name: has invalid format, " <>
                 "should be at least 3 character(s); port: is invalid; " <>
                 "endpoint: has invalid format"
    end
  end

  defp example_config_changeset(overrides) do
    types = %{
      api_key: :string,
      database: :string,
      endpoint: :string,
      event_types: {:array, :string},
      max_message_bytes: :integer,
      name: :string,
      pool_size: :integer,
      port: :integer,
      query_timeout: :integer,
      region: :string,
      retention_days: :integer,
      source_token: :string
    }

    attrs =
      Map.merge(
        %{
          api_key: "lf-api-key-1234567890",
          database: "logs",
          endpoint: "https://clickhouse.internal:8123",
          event_types: ["log", "metric", "trace"],
          max_message_bytes: 1_000_000,
          name: "Primary ClickHouse",
          pool_size: 10,
          port: 8123,
          query_timeout: 5_000,
          region: "us-east-1",
          retention_days: 30,
          source_token: "550e8400-e29b-41d4-a716-446655440000"
        },
        overrides
      )

    {%{}, types}
    |> Changeset.cast(attrs, Map.keys(types))
    |> Changeset.validate_required([:api_key, :database, :endpoint, :name, :source_token])
    |> Changeset.validate_length(:name, min: 3, max: 64)
    |> Changeset.validate_format(:name, ~r/^[[:alnum:] _-]+$/u)
    |> Changeset.validate_exclusion(:database, ["system", "information_schema"],
      message: "is reserved"
    )
    |> Changeset.validate_format(:endpoint, ~r/^https?:\/\//)
    |> Changeset.validate_subset(:event_types, ["log", "metric", "trace"])
    |> Changeset.validate_number(:max_message_bytes, greater_than: 0)
    |> Changeset.validate_number(:pool_size,
      greater_than_or_equal_to: 1,
      less_than_or_equal_to: 100
    )
    |> Changeset.validate_inclusion(:port, 1..65_535)
    |> Changeset.validate_number(:query_timeout,
      greater_than_or_equal_to: 100,
      less_than_or_equal_to: 120_000
    )
    |> Changeset.validate_inclusion(:region, ["us-east-1", "us-west-1", "eu-central-1"])
    |> Changeset.validate_number(:retention_days,
      greater_than_or_equal_to: 1,
      less_than_or_equal_to: 365
    )
    |> Changeset.validate_length(:source_token, is: 36)
  end

  def put_env_or_delete_otherwise(scope, key, value) do
    if value do
      Application.put_env(scope, key, value)
    else
      Application.delete_env(scope, key)
    end
  end
end
