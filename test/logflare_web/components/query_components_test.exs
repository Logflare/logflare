defmodule LogflareWeb.QueryComponentsTest do
  use ExUnit.Case
  import Phoenix.LiveViewTest

  alias LogflareWeb.QueryComponents
  alias GoogleApi.BigQuery.V2.Model.QueryParameter
  alias GoogleApi.BigQuery.V2.Model.QueryParameterType
  alias GoogleApi.BigQuery.V2.Model.QueryParameterValue

  describe "formatted_sql/1" do
    test "formats SQL" do
      html =
        render_component(&QueryComponents.formatted_sql/1, %{
          sql_string: "SELECT id, name FROM users WHERE active = true",
          params: []
        })

      assert html =~ ~r/SELECT\n  id,\n  name\nFROM\n  users\nWHERE\n  active = TRUE/
    end

    test "replaces multiple parameters in order" do
      params = [
        %QueryParameter{
          parameterType: %QueryParameterType{type: "STRING"},
          parameterValue: %QueryParameterValue{value: "active"}
        },
        %QueryParameter{
          parameterType: %QueryParameterType{type: "INTEGER"},
          parameterValue: %QueryParameterValue{value: "100"}
        }
      ]

      html =
        render_component(&QueryComponents.formatted_sql/1, %{
          sql_string: "SELECT * FROM table WHERE status = ? AND count > ?",
          params: params
        })

      # Check for HTML-encoded quotes: &#39; is ' and &quot; is "
      assert html =~ "STATUS = &#39;active&#39;"
      assert html =~ "count &gt; &quot;100&quot;"
    end

    test "includes copy to clipboard button" do
      html =
        render_component(&QueryComponents.formatted_sql/1, %{
          sql_string: "SELECT * FROM table",
          params: []
        })

      assert html =~ "copy"
      assert html =~ "logflare:copy-to-clipboard"
    end

    test "handles float parameters" do
      params = [
        %QueryParameter{
          parameterType: %QueryParameterType{type: "FLOAT"},
          parameterValue: %QueryParameterValue{value: "3.14"}
        }
      ]

      html =
        render_component(&QueryComponents.formatted_sql/1, %{
          sql_string: "SELECT * FROM table WHERE price = ?",
          params: params
        })

      assert html =~ "3.14"
    end
  end
end
