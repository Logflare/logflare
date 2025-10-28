defmodule LogflareWeb.BillingAccountLive.EstimateUsageComponent do
  @moduledoc """
  Liveview component to show a table with an estimate of usage and cost for a given user.
  """
  use LogflareWeb, :live_component
  use Phoenix.HTML
  require Logger

  alias Logflare.Billing.BillingCounts

  def mount(socket), do: {:ok, socket}

  def render(%{user: user} = assigns) do
    end_date = DateTime.utc_now()

    start_date =
      end_date
      |> then(&Date.new!(&1.year, &1.month, &1.day))
      |> Date.beginning_of_month()
      |> DateTime.new!(~T[00:00:00])

    usage = BillingCounts.cumulative_usage(user, start_date, end_date)
    presentation_start_date = Calendar.strftime(start_date, "%b %d")
    presentation_end_date = Calendar.strftime(end_date, "%b %d")

    assigns =
      Map.merge(assigns, %{
        presentation_start_date: presentation_start_date,
        presentation_end_date: presentation_end_date,
        usage: usage
      })

    ~H"""
    <div class="my-3 w-auto">
      <div class="flex flex-col">
        <div>
          Estimate usage between {@presentation_start_date} and {@presentation_end_date}:
        </div>
        <div>Usage: {@usage} inserts</div>
      </div>
    </div>
    """
  end
end
