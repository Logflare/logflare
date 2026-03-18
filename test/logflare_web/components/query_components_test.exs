defmodule LogflareWeb.QueryComponentsTest do
  use LogflareWeb.ConnCase, async: true

  alias GoogleApi.BigQuery.V2.Model.QueryParameter
  alias GoogleApi.BigQuery.V2.Model.QueryParameterType
  alias GoogleApi.BigQuery.V2.Model.QueryParameterValue
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule
  alias LogflareWeb.QueryComponents

  doctest LogflareWeb.QueryComponents, import: true

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
      source_schema_flat_map = SchemaUtils.bq_schema_to_flat_typemap(schema)

      {:ok, source: source, schema: schema, source_schema_flat_map: source_schema_flat_map}
    end

    test "renders a timestamp quick filter link", %{
      source: source,
      schema: schema,
      source_schema_flat_map: flat_map
    } do
      timestamp = 1_609_459_200_000_000

      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "t:last@7day c:count(*) c:group_by(t::hour)",
          node: %{key: "timestamp", path: ["timestamp"], value: timestamp},
          source: source,
          source_schema_flat_map: flat_map,
          lql_schema: schema
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

    test "renders a timestamp quick filter link with minute chart period", %{
      source: source,
      schema: schema,
      source_schema_flat_map: flat_map
    } do
      timestamp = 1_609_459_200_000_000

      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "t:last@7day c:count(*) c:group_by(t::minute)",
          node: %{key: "timestamp", path: ["timestamp"], value: timestamp},
          source: source,
          source_schema_flat_map: flat_map,
          lql_schema: schema
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

    test "renders a metadata quick filter link", %{
      source: source,
      schema: schema,
      source_schema_flat_map: flat_map
    } do
      lql = ~s|m.status:"error"|

      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "",
          node: %{key: "status", path: ["metadata", "status"], value: "error"},
          source: source,
          source_schema_flat_map: flat_map,
          lql_schema: schema
        })

      expected_href = ~p"/sources/#{source}/search?#{%{querystring: lql, tailing?: false}}"

      doc = Floki.parse_document!(html)
      assert Floki.attribute(doc, "a", "href") == [expected_href]
    end

    test "renders a metadata array quick filter link", %{
      source: source,
      schema: schema,
      source_schema_flat_map: flat_map
    } do
      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "",
          node: %{key: "", path: ["metadata", "tags"], value: "popular, tropical, organic"},
          source: source,
          source_schema_flat_map: flat_map,
          lql_schema: schema
        })

      [href] =
        html
        |> Floki.parse_document!()
        |> Floki.attribute("a", "href")

      %URI{query: query} = URI.parse(href)

      assert %{"querystring" => querystring, "tailing?" => "false"} = URI.decode_query(query)

      assert [
               %FilterRule{
                 path: "metadata.tags",
                 operator: :list_includes,
                 value: "popular, tropical, organic"
               }
             ] = Lql.decode!(querystring, schema)
    end

    test "preserves timezone when rendering a quick filter link", %{
      source: source,
      schema: schema,
      source_schema_flat_map: flat_map
    } do
      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "",
          node: %{key: "status", path: ["metadata", "status"], value: "error"},
          source: source,
          source_schema_flat_map: flat_map,
          lql_schema: schema,
          search_params: %{"tz" => "Australia/Brisbane"}
        })

      [href] =
        html
        |> Floki.parse_document!()
        |> Floki.attribute("a", "href")

      %URI{query: query} = URI.parse(href)

      assert %{
               "querystring" => ~s|m.status:"error"|,
               "tailing?" => "false",
               "tz" => "Australia/Brisbane"
             } = URI.decode_query(query)
    end

    test "hides quick filter links until hover", %{
      source: source,
      schema: schema,
      source_schema_flat_map: flat_map
    } do
      [
        {%{key: "timestamp", path: ["timestamp"], value: NaiveDateTime.utc_now()}, []},
        {%{key: "event_message", path: ["event_message"], value: "error"}, []},
        {%{key: "status", path: ["metadata", "status"], value: "error"},
         ["tw-hidden group-hover:tw-inline"]}
      ]
      |> Enum.each(fn {node, class} ->
        html =
          render_component(&QueryComponents.quick_filter/1, %{
            lql: "",
            node: node,
            source: source,
            source_schema_flat_map: flat_map,
            lql_schema: schema
          })

        doc = Floki.parse_document!(html)

        assert Floki.attribute(doc, "a", "class") == class
      end)
    end

    test "omits the quick filter link when value is too long", %{
      source: source,
      schema: schema,
      source_schema_flat_map: flat_map
    } do
      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "",
          node: %{
            key: "message",
            path: ["metadata", "message"],
            value: String.duplicate("a", 501)
          },
          source: source,
          source_schema_flat_map: flat_map,
          lql_schema: schema
        })

      doc = Floki.parse_document!(html)
      assert Floki.find(doc, "a") == []
    end

    test "returns empty HTML when key is not in schema", %{
      source: source,
      schema: schema,
      source_schema_flat_map: flat_map
    } do
      html =
        render_component(&QueryComponents.quick_filter/1, %{
          lql: "",
          node: %{key: "missing", path: ["nonexistent", "path", "missing"], value: "test"},
          source: source,
          source_schema_flat_map: flat_map,
          lql_schema: schema
        })

      doc = Floki.parse_document!(html)
      assert Floki.find(doc, "a") == []
    end
  end
end
