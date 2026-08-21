defmodule LogflareWeb.Api.SourceCsv do
  @moduledoc false

  @spec encode([%{token: String.t() | atom(), name: String.t()}]) :: String.t()
  def encode(sources) do
    (["token,name"] ++ Enum.map(sources, &row/1))
    |> Enum.join("\r\n")
    |> Kernel.<>("\r\n")
  end

  defp row(%{token: token, name: name}), do: escape(token) <> "," <> escape(name)

  defp escape(value) do
    value = to_string(value)

    if String.contains?(value, [",", "\"", "\r", "\n"]) do
      "\"" <> String.replace(value, "\"", "\"\"") <> "\""
    else
      value
    end
  end
end
