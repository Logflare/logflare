defmodule Logflare.Utils do

  def post_a_lot_dev() do
    api_key = "Z0mEDl_ZxB-5"
    source = "5f3c0076-b5c4-4c35-89d3-578282abf469"
    url = "http://localhost:4000/api/logs"
    user_agent = "Test script"
    line = "Derp"

    headers = [
      {"Content-type", "application/json"},
      {"X-API-KEY", api_key},
      {"User-Agent", user_agent}
    ]

    body = Jason.encode!(%{
      log_entry: line,
      source: source,
      })

    for n <- 1..3000 do
      n = HTTPoison.post!(url, body, headers)
      IO.puts(n.status_code)
    end
  end

end
