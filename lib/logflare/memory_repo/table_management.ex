defmodule Logflare.MemoryRepo.TableManagement do
  # TODO: investigate a more efficient way that doesn't require reading all user data on every entry
  # possible options ets select_delete and select_count
  @spec get_ids_for_sorted_records_over_max([map()], {atom(), :desc | :asc}, integer()) ::
          [atom() | integer()]
  def get_ids_for_sorted_records_over_max(rows, {field, order}, n)
      when order in [:asc, :desc] do
    {_, rest_ids} =
      rows
      |> Enum.sort_by(&Map.get(&1, field), order)
      |> Enum.map(fn %{id: id} -> id end)
      |> Enum.split(n)

    rest_ids
  end
end
