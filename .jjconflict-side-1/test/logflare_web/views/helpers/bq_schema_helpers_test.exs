defmodule LogflareWeb.Helpers.BqSchemaTest do
  use LogflareWeb.ConnCase, async: true

  alias Logflare.Google.BigQuery.SchemaUtils
  alias LogflareWeb.Helpers.BqSchema

  describe "format_bq_schema/2" do
    test "formats a BigQuery schema as a flat markdown list" do
      schema =
        TestUtils.build_bq_schema(%{
          "event_message" => "human-readable event message",
          "metadata" => %{
            "tags" => ["tag"],
            "user_id" => 1
          }
        })

      assert BqSchema.format_bq_schema(schema, type: :markdown) ==
               """
               # Logflare source schema

               Use this schema when writing Logflare LQL (https://docs.logflare.app/concepts/lql/)

               - `event_message` STRING Human-readable event message.
               - `id` STRING Event UUID.
               - `metadata` RECORD
               - `metadata.tags` ARRAY<STRING>
               - `metadata.user_id` INTEGER
               - `timestamp` DATETIME Ingest timestamp.\
               """
    end
  end

  describe "format_schema/2" do
    test "formats a flatmap schema as a markdown list" do
      schema_flatmap =
        TestUtils.build_bq_schema(%{
          "event_message" => "human-readable event message",
          "metadata" => %{
            "tags" => ["tag"],
            "user_id" => 1
          }
        })
        |> SchemaUtils.bq_schema_to_flat_typemap()

      assert BqSchema.format_schema(schema_flatmap, type: :markdown) ==
               """
               # Logflare source schema

               Use this schema when writing Logflare LQL (https://docs.logflare.app/concepts/lql/)

               - `event_message` STRING Human-readable event message.
               - `id` STRING Event UUID.
               - `metadata` RECORD
               - `metadata.tags` ARRAY<STRING>
               - `metadata.user_id` INTEGER
               - `timestamp` DATETIME Ingest timestamp.\
               """
    end
  end

  describe "format_timestamp/3" do
    test "formats timestamps with the default log timestamp format" do
      timestamp = 1_234_567_890_000_000

      assert BqSchema.format_timestamp(timestamp, "America/Los_Angeles") ==
               "Fri Feb 13 2009 15:31:30"
    end

    test "formats timestamps with a custom Timex format" do
      timestamp = 1_777_263_766_765_189

      assert BqSchema.format_timestamp(timestamp, "Australia/Brisbane",
               format: "%Y-%m-%d %H:%M:%S"
             ) == "2026-04-27 14:22:46"
    end

    test "falls back to UTC when timezone conversion fails" do
      timestamp = 1_777_263_766_765_189

      assert BqSchema.format_timestamp(timestamp, "Not/A_Zone") ==
               "Mon Apr 27 2026 04:22:46"
    end

    test "falls back to UTC when timezone is missing" do
      timestamp = 1_777_263_766_765_189

      assert BqSchema.format_timestamp(timestamp, nil) ==
               "Mon Apr 27 2026 04:22:46"
    end
  end
end
