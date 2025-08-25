defmodule Logflare.Google.BigQuery.GenUtilsTest do
  use Logflare.DataCase, async: false

  alias Logflare.Google.BigQuery.GenUtils

  doctest GenUtils

  describe "default_table_ttl_days/0" do
    test "returns the default TTL in days" do
      assert GenUtils.default_table_ttl_days() == 7.0
    end
  end

  describe "get_tesla_error_message/1" do
    test "handles `Tesla.Env` with JSON body" do
      env = %Tesla.Env{
        body: Jason.encode!(%{"error" => %{"message" => "Bad request"}})
      }

      assert GenUtils.get_tesla_error_message(env) == "Bad request"
    end

    test "handles `Tesla.Env` with invalid JSON" do
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

  describe "process_bq_errors/2" do
    setup do
      env = Application.get_env(:logflare, :env)
      project_id = Application.get_env(:logflare, Logflare.Google)[:project_id]
      user = insert(:user)

      [env: env, project_id: project_id, user: user]
    end

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

    test "replaces technical table names with source names", %{
      user: user,
      project_id: project_id,
      env: env
    } do
      source = insert(:source, user: user)
      token_with_underscores = source.token |> Atom.to_string() |> String.replace("-", "_")
      technical_name = "#{project_id}.#{user.id}_#{env}.#{token_with_underscores}"

      error = %{
        "message" => "Table #{technical_name} not found"
      }

      result = GenUtils.process_bq_errors(error, user.id)
      assert result["message"] =~ source.name
      refute result["message"] =~ token_with_underscores
    end

    test "processes errors recursively in nested structures", %{
      user: user,
      project_id: project_id,
      env: env
    } do
      source = insert(:source, user: user, name: "my_events")
      token_with_underscores = source.token |> Atom.to_string() |> String.replace("-", "_")
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
end
