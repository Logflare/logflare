defmodule LogflareWeb.Live.DisplayHelpersTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias LogflareWeb.Live.DisplayHelpers

  doctest LogflareWeb.Live.DisplayHelpers, import: true

  describe "sanitize_backend_config/1" do
    property "preserves only allowed keys and masks everything else" do
      config = %{
        async_insert: true,
        batch_timeout: 5000,
        database: "logflare_production",
        hostname: "db.example.com",
        pool_size: 15,
        port: 5432,
        project_id: "my-project-123",
        region: "us-east-1",
        s3_bucket: "logflare-bucket",
        schema: "public",
        storage_region: "eu-west-1",
        table: "log_events",
        url: "https://api.example.com",
        use_simple_schemas: true
      }

      check all key <- atom(:alphanumeric), key not in Map.keys(config) do
        config = Map.put(config, key, "sensitive_value")

        assert DisplayHelpers.sanitize_backend_config(config) == %{
                 key => "**********",
                 async_insert: true,
                 batch_timeout: 5000,
                 database: "logflare_production",
                 hostname: "db.example.com",
                 pool_size: 15,
                 port: 5432,
                 project_id: "my-project-123",
                 region: "us-east-1",
                 s3_bucket: "logflare-bucket",
                 schema: "public",
                 storage_region: "eu-west-1",
                 table: "log_events",
                 url: "https://api.example.com",
                 use_simple_schemas: true
               }
      end
    end
  end
end
