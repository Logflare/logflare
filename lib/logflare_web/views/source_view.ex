defmodule LogflareWeb.SourceView do
  use LogflareWeb, :view

  defdelegate format_bq_schema(source), to: LogflareWeb.Source.TailSearchLV.Utils

  def format_timestamp(timestamp) do
    timestamp
    |> Timex.from_unix(:microsecond)
    |> Timex.format!("%a %b %d %Y %I:%M:%S%p", :strftime)
  end

  def encode_metadata(metadata) do
    Jason.encode!(metadata, pretty: true)
  end

  def generate_search_link(querystring, tailing?) do
    str =
      URI.encode_query(%{
        q: querystring,
        tailing: tailing?
      })

    if str == "" do
      ""
    else
      "?" <> str
    end
  end
end
