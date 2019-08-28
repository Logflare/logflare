defmodule Logflare.Cluster.Utils do
  def get_ui_node_name() do
    case filter_node_list() do
      [ui_node] ->
        {:ok, ui_node}

      [] ->
        {:error, :not_found}
    end
  end

  defp filter_node_list() do
    [Node.self() | Node.list()]
    |> Enum.filter(fn x ->
      case Atom.to_string(x) |> String.split("_") |> Enum.at(1) |> String.to_atom() do
        :ui ->
          true

        _ ->
          false
      end
    end)
  end
end
