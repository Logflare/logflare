defmodule LogflareWeb.BillingAccountLive.EstimateUsageComponentTest do
  use Logflare.DataCase
  alias LogflareWeb.BillingAccountLive.EstimateUsageComponent
  import Phoenix.LiveViewTest
  @endpoint LogflareWeb.Endpoint

  describe "EstimateUsageComponent" do
    setup do
      user = insert(:user, billing_enabled: true)
      plan = insert(:plan)
      end_date = DateTime.utc_now()

      start_date =
        end_date
        |> then(&Date.new!(&1.year, &1.month, &1.day))
        |> Date.beginning_of_month()
        |> DateTime.new!(~T[00:00:00])

      %{
        user: user,
        plan: plan,
        start_date: start_date,
        end_date: end_date
      }
    end

    test "renders element with estimated usage and price for a given period and plan", %{
      user: user,
      plan: plan,
      start_date: start_date,
      end_date: end_date
    } do
      expected_presentation_start_date = Calendar.strftime(start_date, "%b %d")
      expected_presentation_end_date = Calendar.strftime(end_date, "%b %d")

      result = render_component(EstimateUsageComponent, %{user: user, plan: plan})

      assert result =~
               "Estimate usage and cost between #{expected_presentation_start_date} and #{expected_presentation_end_date}:"

      assert Regex.match?(~r/Usage: [\d]+ inserts/, result)
      assert Regex.match?(~r/Estimated Cost: [\d]+ USD/, result)
    end
  end
end
