defmodule LogflareWeb.Logs.PayloadTestUtils do
  def standard_metadata(:cloudflare) do
    %{
      "request" => %{
        "cf" => %{
          "asn" => 15_169,
          "clientTrustScore" => 1,
          "colo" => "DFW",
          "country" => "US",
          "httpProtocol" => "HTTP/1.1",
          "requestPriority" => "",
          "tlsCipher" => "ECDHE-ECDSA-AES128-GCM-SHA256",
          "tlsClientAuth" => %{
            "certFingerprintSHA1" => "",
            "certIssuerDN" => "",
            "certIssuerDNLegacy" => "",
            "certIssuerDNRFC2253" => "",
            "certNotAfter" => "",
            "certNotBefore" => "",
            "certPresented" => "0",
            "certSerial" => "",
            "certSubjectDN" => "",
            "certSubjectDNLegacy" => "",
            "certSubjectDNRFC2253" => "",
            "certVerified" => "NONE"
          },
          "tlsVersion" => "TLSv1.2"
        },
        "headers" => %{
          "accept" =>
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8,text/html,application/xhtml+xml,application/signed-exchange;v=b3,application/xml;q=0.9,*/*;q=0.8",
          "accept_encoding" => "gzip",
          "amp_cache_transform" => "google;v=\"1\"",
          "cf_connecting_ip" => "66.249.73.71",
          "cf_ipcountry" => "US",
          "cf_ray" => "4d620c1458049b7f",
          "cf_visitor" => "{\"scheme\" =>\"https\"}",
          "connection" => "Keep-Alive",
          "cookie" => "__cfduid=d31fa453a1f0c2bcf2f944a98b794bed41557723400",
          "from" => "googlebot(at)googlebot.com",
          "host" => "logflare.app",
          "user_agent" =>
            "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
          "x_forwarded_proto" => "https",
          "x_real_ip" => "66.249.73.71"
        },
        "method" => "GET",
        "url" => "https://logflare.app/"
      },
      "response" => %{
        "headers" => %{
          "cache_control" => "max-age=0, private, must-revalidate",
          "cf_ray" => "4d620c1456db9b7f-DFW",
          "connection" => "keep-alive",
          "content_type" => "text/html; charset=utf-8",
          "cross_origin_window_policy" => "deny",
          "date" => "Mon, 13 May 2019 04:56:40 GMT",
          "expect_ct" =>
            "max-age=604800, report-uri=\"https://report-uri.cloudflare.com/cdn-cgi/beacon/expect-ct\"",
          "server" => "cloudflare",
          "transfer_encoding" => "chunked",
          "vary" => "Accept-Encoding",
          "x_content_type_options" => "nosniff",
          "x_download_options" => "noopen",
          "x_frame_options" => "SAMEORIGIN",
          "x_permitted_cross_domain_policies" => "none",
          "x_xss_protection" => "1; mode=block"
        },
        "origin_time" => 174,
        "status_code" => 200
      }
    }
  end

  def standard_metadata(:elixir_logger_exception) do
    %{
      "pid" => "<0.234.0>",
      "stacktrace" => [
        %{
          "arity_or_args" => 0,
          "file" => "lib/logflare_pinger/log_pinger.ex",
          "function" => "-handle_info/2-fun-0-/0",
          "line" => 18,
          "module" => "LogflareLoggerPinger.Server"
        },
        %{
          "arity_or_args" => 2,
          "file" => "lib/logflare_pinger/log_pinger.ex",
          "function" => "-handle_info/2-fun-0-/0",
          "line" => 25,
          "module" => "LogflareLoggerPinger.Server"
        }
      ]
    }
  end
end
