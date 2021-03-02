defmodule Logflare.Mnesia do
  defmodule Indexes do
    alias Logflare.Mnesia
    use TypedStruct

    defmodule Index do
      typedstruct do
        field :tab, atom(), enforce: true
        field :position, non_neg_integer, enforce: true
        field :type, atom, enforce: true
        field :storage_type, atom, enforce: true
        field :ref, reference, enforce: true
        field :memory, integer
        field :size, integer
      end
    end

    @spec list_indexes(atom()) :: [Index.t()]
    def list_indexes(tab) do
      {:index, _type, indexes} = Mnesia.table_info(tab).index_info

      for {{pos, type}, {storage_type, ref}} <- indexes do
        %Index{tab: tab, position: pos, type: type, storage_type: storage_type, ref: ref}
      end
    end

    @spec refresh_index_data(Index.t() | [Index.t()]) :: Index.t()
    def refresh_index_data(indexes) when is_list(indexes) do
      for i <- indexes, do: refresh_index_data(i)
    end

    def refresh_index_data(%Index{ref: ref} = index) do
      ets_data =
        ref
        |> ETS.Set.wrap_existing!()
        |> ETS.Set.info!()
        |> Map.new()

      %{index | memory: ets_data.memory, size: ets_data.size}
    end
  end

  @type table_info_key :: atom() | {atom(), term}

  def clear_table(tab) when is_atom(tab) do
    :mnesia.clear_table(tab)
  end

  @spec table_info(atom(), atom | [atom()]) :: %{atom() => term}
  def table_info(tab, options \\ :all)

  def table_info(tab, options) do
    tab
    |> :mnesia.table_info(options)
    |> Map.new()
  end

  def indexes(tab) when is_atom(tab) do
    {:index, _type, indexes} = table_info(tab).index_info
    indexes
  end
end
