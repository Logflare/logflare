defmodule LogflareWeb.SourceBqSchemaComponentTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Google.BigQuery.SchemaUtils
  alias LogflareWeb.SourceBqSchemaComponent

  describe "render/1" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)
      source = insert(:source, user: user)

      %{source: source}
    end

    test "renders and copies schema from the flatmap schema", %{source: source} do
      bq_schema =
        TestUtils.build_bq_schema(%{
          "metadata" => %{"user_id" => 123, "tags" => ["testing"]},
          "event_message" => "message"
        })

      source_schema =
        insert(:source_schema,
          source: source,
          bigquery_schema: bq_schema,
          schema_flat_map:
            bq_schema
            |> SchemaUtils.bq_schema_to_flat_typemap()
            |> Map.put("flatmap_only", :string)
        )

      html = render_component(SourceBqSchemaComponent, %{id: "schema", source: source})

      assert html =~ "<kbd>flatmap_only</kbd>"
      assert html =~ "data-clipboard-text=\"flatmap_only\""
      assert html =~ "- `flatmap_only` STRING"
      assert html =~ "- `metadata.tags` ARRAY&lt;STRING&gt;"
      assert html =~ "- `metadata.user_id` INTEGER"
      assert source_schema.schema_flat_map["flatmap_only"] == :string
    end

    test "renders the default flatmap schema when source schema is missing", %{source: source} do
      html = render_component(SourceBqSchemaComponent, %{id: "schema", source: source})

      assert html =~ "<kbd>event_message</kbd>"
      assert html =~ "- `event_message` STRING Human-readable event message."
    end
  end
end
