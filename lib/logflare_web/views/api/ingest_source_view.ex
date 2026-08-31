defmodule LogflareWeb.Api.IngestSourceView do
  use LogflareWeb, :view

  def render("index.csv", %{sources: sources}) do
    [
      "token,name\r\n"
      | Enum.map(sources, fn %{token: token, name: name} ->
          [csv_field(token), ",", csv_field(name), "\r\n"]
        end)
    ]
  end

  defp csv_field(value) do
    value = to_string(value)

    if String.contains?(value, [",", "\"", "\r", "\n"]) do
      ["\"", String.replace(value, "\"", "\"\""), "\""]
    else
      value
    end
  end
end
