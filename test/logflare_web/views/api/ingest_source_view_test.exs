defmodule LogflareWeb.Api.IngestSourceViewTest do
  use ExUnit.Case, async: true

  alias LogflareWeb.Api.IngestSourceView

  test "renders JSON sources unchanged" do
    sources = [%{token: :source_a, name: "Alpha"}]

    assert IngestSourceView.render("index.json", %{sources: sources}) == sources
  end

  test "renders exact RFC 4180 CSV output" do
    sources = [
      %{token: :source_a, name: "Alpha"},
      %{token: :source_b, name: "comma, \"quote\"\nline"}
    ]

    csv =
      IngestSourceView.render("index.csv", %{sources: sources})
      |> IO.iodata_to_binary()

    assert csv ==
             "token,name\r\nsource_a,Alpha\r\nsource_b,\"comma, \"\"quote\"\"\nline\"\r\n"
  end
end
