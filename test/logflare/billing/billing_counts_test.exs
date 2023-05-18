defmodule Logflare.Billing.BillingCountsTest do
  use Logflare.DataCase
  alias Logflare.Billing.BillingCounts

  setup do
    user = insert(:user)

    _non_matching_user = insert(:billing_counts, inserted_at: ~U[2023-02-01 00:00:00Z])

    _outside_boundary = insert(:billing_counts, user: user, inserted_at: ~U[2023-01-31 00:00:00Z])

    _outside_boundary = insert(:billing_counts, user: user, inserted_at: ~U[2023-03-01 00:00:00Z])

    inside_boundary_0 = insert(:billing_counts, user: user, inserted_at: ~U[2023-02-17 00:00:00Z])

    inside_boundary_1 = insert(:billing_counts, user: user, inserted_at: ~U[2023-02-17 00:00:00Z])

    inside_boundary_2 = insert(:billing_counts, user: user, inserted_at: ~U[2023-02-10 00:00:00Z])

    %{
      user: user,
      inside_boundary_0: inside_boundary_0,
      inside_boundary_1: inside_boundary_1,
      inside_boundary_2: inside_boundary_2
    }
  end

  describe "timeseries/3" do
    test "fetches billing counts for a certain period in a time series format", %{
      user: user,
      inside_boundary_0: inside_boundary_0,
      inside_boundary_1: inside_boundary_1,
      inside_boundary_2: inside_boundary_2
    } do
      result = BillingCounts.timeseries(user, ~U[2023-02-01 00:00:00Z], ~U[2023-02-28 00:00:00Z])

      days =
        Date.range(~D[2023-02-01], ~D[2023-02-28]) |> Enum.map(&DateTime.new!(&1, ~T[00:00:00]))

      assert Enum.all?(result, fn [d | _] ->
               Enum.find(days, &(DateTime.compare(&1, d) == :eq))
             end)

      expected_sum_day = inside_boundary_0.count + inside_boundary_1.count

      assert result
             |> Enum.find(fn [d | _] -> DateTime.compare(d, ~U[2023-02-17 00:00:00Z]) == :eq end)
             |> then(fn [_, sum | _] -> sum end) == expected_sum_day

      assert result
             |> Enum.find(fn [d | _] -> DateTime.compare(d, ~U[2023-02-10 00:00:00Z]) == :eq end)
             |> then(fn [_, sum | _] -> sum end) == inside_boundary_2.count
    end
  end

  describe "cumulative_usage/3" do
    test "returns cumulative total for a given period", %{
      user: user,
      inside_boundary_0: inside_boundary_0,
      inside_boundary_1: inside_boundary_1,
      inside_boundary_2: inside_boundary_2
    } do
      result =
        BillingCounts.cumulative_usage(user, ~U[2023-02-01 00:00:00Z], ~U[2023-02-28 00:00:00Z])

      expected = inside_boundary_0.count + inside_boundary_1.count + inside_boundary_2.count

      assert result == expected
    end
  end
end
