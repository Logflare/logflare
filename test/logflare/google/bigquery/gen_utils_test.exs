defmodule Logflare.Google.BigQuery.GenUtilsTest do
  use Logflare.DataCase, async: true

  alias Logflare.Google.BigQuery.GenUtils

  describe "process_bq_errors/2" do
    test "handles atom errors" do
      result = GenUtils.process_bq_errors(:timeout, 123)
      assert result == %{"message" => :timeout}
    end

    test "handles map errors with message" do
      error = %{"message" => "Table not found", "code" => 404}
      result = GenUtils.process_bq_errors(error, 123)
      assert result["message"] == "Table not found"
      assert result["code"] == 404
    end

    test "handles nested errors" do
      error = %{
        "message" => "Main error",
        "errors" => [
          %{"message" => "Sub error 1"},
          %{"message" => "Sub error 2"}
        ]
      }

      result = GenUtils.process_bq_errors(error, 123)
      assert result["message"] == "Main error"
      assert length(result["errors"]) == 2
    end

    test "replaces technical table names with source names" do
      user = insert(:user)
      source = insert(:source, user: user)

      token_with_underscores = String.replace(Atom.to_string(source.token), "-", "_")
      project_id = Application.get_env(:logflare, Logflare.Google)[:project_id]
      env = Application.get_env(:logflare, :env)
      technical_name = "#{project_id}.#{user.id}_#{env}.#{token_with_underscores}"

      error = %{
        "message" => "Table #{technical_name} not found"
      }

      result = GenUtils.process_bq_errors(error, user.id)
      assert result["message"] =~ source.name
      refute result["message"] =~ token_with_underscores
    end

    test "processes errors recursively in nested structures" do
      user = insert(:user)
      source = insert(:source, user: user, name: "my_events")

      token_with_underscores = String.replace(Atom.to_string(source.token), "-", "_")
      project_id = Application.get_env(:logflare, Logflare.Google)[:project_id]
      env = Application.get_env(:logflare, :env)
      technical_name = "#{project_id}.#{user.id}_#{env}.#{token_with_underscores}"

      error = %{
        "message" => "Query failed",
        "errors" => [
          %{"message" => "Table #{technical_name} not found"},
          %{"message" => "Permission denied on #{technical_name}"}
        ]
      }

      result = GenUtils.process_bq_errors(error, user.id)
      assert result["errors"] |> Enum.at(0) |> Map.get("message") =~ "my_events"
      assert result["errors"] |> Enum.at(1) |> Map.get("message") =~ "my_events"
    end
  end

  describe "format_key/1" do
    test "formats string labels correctly" do
      assert GenUtils.format_key("My Label Name") == "my_label_name"
      assert GenUtils.format_key("UPPERCASE") == "uppercase"
    end

    test "formats integer labels" do
      assert GenUtils.format_key(123) == "123"
    end

    test "formats atom labels" do
      assert GenUtils.format_key(:my_atom) == "my_atom"
    end

    test "truncates long labels to 62 characters" do
      long_label = String.duplicate("a", 100)
      result = GenUtils.format_key(long_label)
      assert String.length(result) == 62
    end

    test "replaces spaces with underscores" do
      assert GenUtils.format_key("label with spaces") == "label_with_spaces"
    end
  end

  describe "get_tesla_error_message/1" do
    test "handles Tesla.Env with JSON body" do
      env = %Tesla.Env{
        body: Jason.encode!(%{"error" => %{"message" => "Bad request"}})
      }

      assert GenUtils.get_tesla_error_message(env) == "Bad request"
    end

    test "handles Tesla.Env with invalid JSON" do
      env = %Tesla.Env{body: "not json"}
      result = GenUtils.get_tesla_error_message(env)
      assert result =~ "not json"
    end

    test "handles known atom errors" do
      assert GenUtils.get_tesla_error_message(:emfile) == "emfile"
      assert GenUtils.get_tesla_error_message(:timeout) == "timeout"
      assert GenUtils.get_tesla_error_message(:closed) == "closed"
    end

    test "inspects unknown errors" do
      assert GenUtils.get_tesla_error_message({:unknown, :error}) =~ "unknown"
    end
  end

  describe "default_table_ttl_days/0" do
    test "returns the default TTL in days" do
      assert GenUtils.default_table_ttl_days() == 7.0
    end
  end
end
