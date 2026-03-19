defmodule Logflare.Profiling do
  alias Logflare.LogEvent

  @iterations 10
  @events_per_iter 250

  def run_legacy() do
    # Source set to use legacy API
    source = Logflare.Sources.get_source_by_token(:"0690209c-fd27-42e8-b07b-57414e656c58")

    for _ <- 1..@iterations do
      events = gen_log_events(source, @events_per_iter)
      Logflare.Backends.ingest_logs(events, source)
    end
  end

  def run_finch() do
    # Source set to use storage write API
    source = Logflare.Sources.get_source_by_token(:"22499191-a99e-447c-bc68-14d745e7d784")

    :ok = Application.put_env(:logflare, :storage_write_impl, :finch)

    for _ <- 1..@iterations do
      events = gen_log_events(source, @events_per_iter)
      Logflare.Backends.ingest_logs(events, source)
    end
  end

  def run_grpc() do
    # Source set to use storage write API
    source = Logflare.Sources.get_source_by_token(:"22499191-a99e-447c-bc68-14d745e7d784")

    :ok = Application.put_env(:logflare, :storage_write_impl, :mint)

    for _ <- 1..@iterations do
      events = gen_log_events(source, @events_per_iter)
      Logflare.Backends.ingest_logs(events, source)
    end
  end

  def gen_log_events(source, num, type \\ :nested) do
    gen =
      case type do
        :flat -> &gen_flat_body/1
        :nested -> &gen_nested_body/1
      end

    for i <- 1..num, do: LogEvent.make(gen.(i), %{source: source})
  end

  defp gen_flat_body(i) do
    %{
      "timestamp" => System.os_time(:microsecond) - 10000 + i,
      "event_message" => "GET /api/v1/users 200",
      "status_code" => 200,
      "method" => "GET",
      "path" => "/api/v1/users",
      "host" => "example.supabase.co",
      "duration_ms" => 42
    }
  end

  defp gen_nested_body(i) do
    %{
      "timestamp" => System.os_time(:microsecond) - 10000 + i,
      "event_message" =>
        "POST | 404 | 33.254.251.15 | https://zzzenjkohrkaatgpywnz#{Enum.random(1..1000)}.supabase.co/rest/v1/rpc/set_active_session",
      "status_code" => 404,
      "method" => "POST",
      "project" => "zzzenjkohrkaatgpywnz",
      "origin_time" => 367,
      "request" => %{
        "method" => "POST",
        "path" => "/rest/v1/rpc/set_active_session",
        "host" => "zzzenjkohrkaatgpywnz.supabase.co",
        "headers" => %{
          "accept" => "*/*",
          "cf_ray" => "9ee9a1c5fdfa3b4b",
          "content_type" => "application/json",
          "user_agent" => "Deno/2.1.4 (variant; SupabaseEdgeRuntime/1.69.25)"
        },
        "cf" => %{
          "country" => "IN",
          "city" => "Mumbai",
          "continent" => "AS",
          "colo" => "BOM",
          "asn" => 16_509,
          "httpProtocol" => "HTTP/2",
          "tlsVersion" => "TLSv1.3",
          "tlsCipher" => "AEAD-AES256-GCM-SHA384",
          "botManagement" => %{
            "score" => 22,
            "verifiedBot" => false,
            "corporateProxy" => false,
            "ja3Hash" => "8a64967e35f306b9a5f5cfe592dd153e"
          }
        }
      },
      "response" => %{
        "status_code" => 404,
        "origin_time" => 367,
        "headers" => %{
          "content_type" => "application/json; charset=utf-8",
          "cf_ray" => "99f7a1c7c2c33c4e-BOM"
        }
      }
    }
  end
end
