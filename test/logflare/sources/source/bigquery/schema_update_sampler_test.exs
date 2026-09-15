defmodule Logflare.Sources.Source.BigQuery.SchemaUpdateSamplerTest do
  use ExUnit.Case, async: false

  alias Logflare.Sources.Source.BigQuery.SchemaUpdateSampler

  defp unique_token,
    do: String.to_atom("schema_update_sampler_test_#{System.unique_integer([:positive])}")

  describe "sample?/1" do
    test "a source never seen before samples on its first call" do
      assert SchemaUpdateSampler.sample?(unique_token())
    end

    test "a high local rate from a completed window drives the effective sample rate down" do
      token = unique_token()

      # A window's rate is only known once it *completes* — a call within
      # the still-running first window always samples at 1.0 (see the
      # module's rolling-window design), so this simulates a just-completed
      # window with a high rate directly, rather than relying on enough
      # real wall-clock time passing in the test itself.
      SchemaUpdateSampler.sample?(token)
      now = System.monotonic_time(:millisecond)
      :ets.insert(:schema_update_sampler, {token, now - 1_001, 2_000, 0})

      sampled = for _ <- 1..2_000, do: SchemaUpdateSampler.sample?(token)
      sampled_count = Enum.count(sampled, & &1)

      assert sampled_count < 100,
             "expected sampling to have backed off after a high local rate, got #{sampled_count}/2000"
    end

    test "a gap longer than the window resets the rate instead of carrying it forward" do
      token = unique_token()

      for _ <- 1..2_000, do: SchemaUpdateSampler.sample?(token)

      # Force the next call to land in a fresh window without actually
      # sleeping a full second in the test: insert a stale window directly.
      :ets.insert(
        :schema_update_sampler,
        {token, System.monotonic_time(:millisecond) - 2_000, 1, 0.001}
      )

      # The stale window's near-zero rate must not carry forward — its first
      # call in the new window should already be resampling near 1.0 (only
      # decaying again once *this* window accumulates real volume).
      assert SchemaUpdateSampler.sample?(token)
    end
  end
end
