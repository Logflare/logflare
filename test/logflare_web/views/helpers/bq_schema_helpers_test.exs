defmodule LogflareWeb.Helpers.BqSchemaTest do
  use LogflareWeb.ConnCase, async: true

  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias LogflareWeb.Helpers.BqSchema

  describe "format_bq_schema/2" do
    test "formats a BigQuery schema as a markdown list" do
      schema = %TS{
        fields: [
          %TFS{name: "event_message", type: "STRING", mode: "REQUIRED"},
          %TFS{
            name: "metadata",
            type: "RECORD",
            mode: "NULLABLE",
            fields: [
              %TFS{name: "tags", type: "STRING", mode: "REPEATED"},
              %TFS{name: "user_id", type: "INTEGER", mode: "NULLABLE"}
            ]
          }
        ]
      }

      assert BqSchema.format_bq_schema(schema, type: :markdown) ==
               """
               # Logflare source schema

               Use this schema when writing Logflare LQL (https://docs.logflare.app/concepts/lql/)

               - `event_message` STRING Human-readable event message.
               - `metadata` RECORD
                 - `tags` ARRAY<STRING>
                 - `user_id` INTEGER\
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
