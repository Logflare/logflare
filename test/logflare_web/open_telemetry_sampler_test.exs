defmodule LogflareWeb.OpenTelemetrySamplerTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import LogflareWeb.OpenTelemetrySampler, only: [should_sample: 7]

  setup do
    prev_ingest = Application.get_env(:logflare, :ingest_sample_ratio)
    prev_endpoint = Application.get_env(:logflare, :endpoint_sample_ratio)
    Application.put_env(:logflare, :ingest_sample_ratio, 1.0)
    Application.put_env(:logflare, :endpoint_sample_ratio, 1.0)

    on_exit(fn ->
      Application.put_env(:logflare, :ingest_sample_ratio, prev_ingest)
      Application.put_env(:logflare, :endpoint_sample_ratio, prev_endpoint)
    end)
  end

  doctest LogflareWeb.OpenTelemetrySampler
end
