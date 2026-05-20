defmodule LogflareWeb.Helpers.BqSchemaTest do
  use LogflareWeb.ConnCase, async: true

  alias LogflareWeb.Helpers.BqSchema

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
