defmodule LogflareWeb.BillingAccountLive.ChartComponent do
  @moduledoc """
  Billing edit LiveView
  """

  use LogflareWeb, :live_component
  use Phoenix.HTML

  alias Contex.{BarChart, Plot, Dataset}

  require Logger

  def preload(assigns) when is_list(assigns) do
    assigns
  end

  def mount(socket) do
    IO.inspect(socket)
    {:ok, socket}
  end

  def update(%{counter: counter} = _assigns, socket) do
    socket =
      socket
      |> assign(
        chart_options: %{
          categories: 10,
          series: 4,
          type: :stacked,
          orientation: :vertical,
          show_selected: "no",
          title: nil,
          colour_scheme: "themed"
        }
      )
      |> assign(counter: counter)
      |> make_test_data()

    socket =
      case connected?(socket) do
        true ->
          Process.send_after(self(), {:chart_tick, counter + 1}, 100)
          assign(socket, show_chart: true)

        false ->
          assign(socket, show_chart: false)
      end

    {:ok, socket}
  end

  def render(assigns) do
    ~L"""
    <div id="billing-chart">
      <%= basic_plot(@chart_data, @chart_options) %>
    </div>
    """
  end

  def basic_plot(test_data, chart_options) do
    options = [
      mapping: %{category_col: "Category", value_cols: chart_options.series_columns},
      type: chart_options.type,
      orientation: chart_options.orientation,
      colour_palette: :default
    ]

    plot_content =
      BarChart.new(test_data, options)
      |> BarChart.force_value_range({0, chart_options.series * 2.0})

    plot =
      Plot.new(400, 125, plot_content)
      |> Plot.titles(chart_options.title, nil)

    Plot.to_svg(plot)
  end

  defp make_test_data(socket) do
    options = socket.assigns.chart_options
    series = options.series
    categories = options.categories
    counter = socket.assigns.counter

    data =
      1..categories
      |> Enum.map(fn cat ->
        series_data =
          for s <- 1..series do
            abs(1 + :math.sin((counter + cat + s) / 5.0))
          end

        ["Category #{cat}" | series_data]
      end)

    series_cols =
      for i <- 1..series do
        "Series #{i}"
      end

    test_data = Dataset.new(data, ["Category" | series_cols])

    options = Map.put(options, :series_columns, series_cols)

    assign(socket, chart_data: test_data, chart_options: options)
  end

  def make_test_data_for_preload() do
    options = default_chart_options()
    series = options.series
    categories = options.categories
    counter = 0

    data =
      1..categories
      |> Enum.map(fn cat ->
        series_data =
          for s <- 1..series do
            abs(1 + :math.sin((counter + cat + s) / 5.0))
          end

        ["Category #{cat}" | series_data]
      end)

    series_cols =
      for i <- 1..series do
        "Series #{i}"
      end

    Dataset.new(data, ["Category" | series_cols])
  end

  def make_options() do
    options = default_chart_options()
    series = options.series

    series_cols =
      for i <- 1..series do
        "Series #{i}"
      end

    Map.put(options, :series_columns, series_cols)
  end

  defp default_chart_options() do
    %{
      categories: 10,
      series: 4,
      type: :stacked,
      orientation: :vertical,
      show_selected: "no",
      title: nil,
      colour_scheme: "themed"
    }
  end
end
