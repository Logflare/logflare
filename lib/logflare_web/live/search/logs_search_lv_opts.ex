defmodule LogflareWeb.Source.SearchLV.SearchOpts do
  @moduledoc false
  use TypedEctoSchema
  import Ecto.Changeset

  @primary_key false
  typed_embedded_schema do
    field :tailing?, :boolean, default: true
    field :querystring, :string, default: "", nil: false
    field :chart_aggregate, Ecto.Atom, default: :count
    field :chart_period, Ecto.Atom, default: :minute
  end

  def new(search_opts \\ %{}, params) do
    %{data: data, changes: changes, valid?: valid?, errors: errors} =
      __MODULE__
      |> struct(search_opts)
      |> cast(prepare(params), __schema__(:fields))
      |> validate_inclusion(:chart_period, ~w(day hour minute second)a)
      |> validate_inclusion(:chart_aggregate, ~w(sum count avg)a)

    if valid? do
      {:ok, Map.merge(data, changes)}
    else
      {:error, errors}
    end
  end

  def update_chart_ops() do
  end

  def prepare(params) do
    params
    |> case do
      %{"querystring" => ""} = p ->
        %{p | "querystring" => "chart:aggregate@count chart:period@minute"}

      %{"querystring" => _} = p ->
        p

      %{"q" => q} = p ->
        Map.put(p, "querystring", q)

      p ->
        p
    end
    |> update_if_exists("chart_aggregate", &String.to_existing_atom/1)
    |> update_if_exists("chart_period", &String.to_existing_atom/1)
  end

  def update_if_exists(map, key, fun) do
    if map[key] do
      Map.update!(map, key, fun)
    else
      map
    end
  end
end
