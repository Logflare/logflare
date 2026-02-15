defmodule LogflareWeb.QueryComponentsTest do
  use LogflareWeb.ConnCase, async: true

  alias GoogleApi.BigQuery.V2.Model.QueryParameter
  alias GoogleApi.BigQuery.V2.Model.QueryParameterType
  alias GoogleApi.BigQuery.V2.Model.QueryParameterValue
  alias LogflareWeb.QueryComponents

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

    test "ensures table names surrounded by backticks" do
      [
        "SELECT t0.timestamp, t0.id FROM `logflare-dev-464423.1_dev.9db56741_41ca_4fe8_8c05_051a76a4c5d6` AS t0",
        "SELECT t0.timestamp, t0.id FROM `logflare-dev-464423`.1_dev.9db56741_41ca_4fe8_8c05_051a76a4c5d6 AS t0"
      ]
      |> Enum.each(fn sql_string ->
        html =
          render_component(&QueryComponents.formatted_sql/1, %{
            sql_string: sql_string,
            params: []
          })

        assert html =~
                 "`logflare-dev-464423.1_dev.9db56741_41ca_4fe8_8c05_051a76a4c5d6`"
      end)
    end
  end

  describe "quick_filter/1" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      schema =
        Logflare.TestUtils.build_bq_schema(%{
          "metadata" => %{
            "status" => "ok",
            "level" => "info",
            "message" => "hello",
            "endpoint" => "/api",
            "user_id" => "123",
            "user" => %{
              "profile" => %{
                "id" => "abc"
              }
            },
            "tags" => ["popular, tropical, organic"],
            "counts" => [12],
            "flags" => [true]
          }
        })

      insert(:source_schema, source: source, bigquery_schema: schema)
      {:ok, source: source}
    end

    test "renders a timestamp quick filter link", %{source: source} do
      timestamp = 1_609_459_200_000_000

      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "t:last@7day c:count(*) c:group_by(t::hour)",
          node: %{key: "timestamp", path: [], value: timestamp},
          source: source
        })

      [href] =
        html
        |> Floki.parse_document!()
        |> Floki.attribute("a", "href")

      %URI{query: query} = URI.parse(href)

      assert %{
               "querystring" =>
                 "t:{2020..2021}-{12..01}-{31..01}T{23..01}:00:00 c:count(*) c:group_by(t::hour)",
               "tailing?" => "false"
             } = query |> URI.decode_query()
    end

    test "renders a timestamp quick filter link with minute chart period", %{source: source} do
      timestamp = 1_609_459_200_000_000

      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "t:last@7day c:count(*) c:group_by(t::minute)",
          node: %{key: "timestamp", path: [], value: timestamp},
          source: source
        })

      [href] =
        html
        |> Floki.parse_document!()
        |> Floki.attribute("a", "href")

      %URI{query: query} = URI.parse(href)

      assert %{
               "querystring" =>
                 "t:{2020..2021}-{12..01}-{31..01}T{23..00}:{59..01}:00 c:count(*) c:group_by(t::minute)",
               "tailing?" => "false"
             } = query |> URI.decode_query()
    end

    test "renders a metadata quick filter link", %{source: source} do
      lql = ~s|m.status:"error"|

      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "",
          node: %{key: "status", path: ["metadata"], value: "error"},
          source: source
        })

      expected_href = ~p"/sources/#{source}/search?#{%{querystring: lql, tailing?: false}}"

      doc = Floki.parse_document!(html)
      assert Floki.attribute(doc, "a", "href") == [expected_href]
    end

    test "tailing? param", %{source: source} do
      lql = ~s|m.status:"error"|

      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "",
          node: %{key: "status", path: ["metadata"], value: "error"},
          source: source,
          is_tailing: true
        })

      expected_href = ~p"/sources/#{source}/search?#{%{querystring: lql, tailing?: true}}"

      doc = Floki.parse_document!(html)
      assert Floki.attribute(doc, "a", "href") == [expected_href]
    end

    test "omits the quick filter link when value is too long", %{source: source} do
      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "",
          node: %{key: "message", path: ["metadata"], value: String.duplicate("a", 501)},
          source: source
        })

      doc = Floki.parse_document!(html)
      assert Floki.find(doc, "a") == []
    end

    test "returns empty HTML when key is not in schema", %{source: source} do
      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "",
          node: %{key: "missing", path: ["nonexistent", "path"], value: "test"},
          source: source
        })

      doc = Floki.parse_document!(html)
      assert Floki.find(doc, "a") == []
    end
  end
end
