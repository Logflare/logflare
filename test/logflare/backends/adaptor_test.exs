defmodule Logflare.Backends.AdaptorTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend

  describe "sanitize_config_for_display/1" do
    test "delegates to the adaptor's callback" do
      backend = %Backend{
        type: :postgres,
        config: %{url: "postgresql://user:secret123@localhost:5432/db", password: "secret123"}
      }

      assert %{url: "postgresql://user:REDACTED@localhost:5432/db", password: "**********"} ==
               Adaptor.sanitize_config_for_display(backend)
    end

    test "masks every value when the adaptor does not implement the callback" do
      backend = %Backend{
        type: :incidentio,
        config: %{api_token: "secret", alert_source_config_id: "123"}
      }

      assert %{api_token: "**********", alert_source_config_id: "**********"} ==
               Adaptor.sanitize_config_for_display(backend)
    end

    test "returns an empty map when config is not set" do
      assert %{} == Adaptor.sanitize_config_for_display(%Backend{type: :clickhouse})
    end
  end

  describe "mask_config_values/2" do
    test "masks all values except those under allowed keys" do
      config = %{database: "logs", password: "secret123", port: 5432}

      assert %{database: "logs", password: "**********", port: 5432} ==
               Adaptor.mask_config_values(config, [:database, :port])
    end

    test "masks everything when no keys are allowed" do
      config = %{api_token: "secret"}

      assert %{api_token: "**********"} == Adaptor.mask_config_values(config, [])
    end
  end
end
