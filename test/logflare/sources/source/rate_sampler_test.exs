defmodule Logflare.Sources.Source.RateSamplerTest do
  use ExUnit.Case, async: false

  alias Logflare.Sources.Source.RateSampler

  defp unique_token,
    do: String.to_atom("rate_sampler_test_#{System.unique_integer([:positive])}")

  describe "sample?/1" do
    test "a source never bumped before samples on its first call" do
      assert RateSampler.sample?(unique_token())
    end

    test "a high local rate from a completed window drives the effective sample rate down" do
      token = unique_token()

      # A window's rate is only known once it *completes*, and only bump/2
      # rotates a stale window into a freshly-computed rate (sample?/1 is a
      # pure read and never rotates one on its own) -- so this simulates a
      # just-completed high-volume window directly, then bumps once more to
      # trigger the rollover, rather than relying on real wall-clock time.
      now = System.monotonic_time(:millisecond)
      :ets.insert(:source_rate_sampler, {token, now - 1_001, 2_000, 0})
      RateSampler.bump(token, 1)

      sampled = for _ <- 1..2_000, do: RateSampler.sample?(token)
      sampled_count = Enum.count(sampled, & &1)

      assert sampled_count < 100,
             "expected sampling to have backed off after a high local rate, got #{sampled_count}/2000"
    end

    test "a gap longer than the window resets the rate instead of carrying it forward" do
      token = unique_token()

      RateSampler.bump(token, 2_000)

      # Force the next call to land in a fresh window without actually
      # sleeping a full second in the test: insert a stale window directly.
      :ets.insert(
        :source_rate_sampler,
        {token, System.monotonic_time(:millisecond) - 2_000, 1, 0.001}
      )

      # The stale window's near-zero rate must not carry forward -- bumping
      # again in the new window should already sample near 1.0 (only decaying
      # again once *this* window accumulates real volume).
      RateSampler.bump(token, 1)
      assert RateSampler.sample?(token)
    end

    test "sample?/1 never mutates the counter -- only bump/2 does" do
      token = unique_token()

      RateSampler.bump(token, 5)
      before = :ets.lookup(:source_rate_sampler, token)

      for _ <- 1..50, do: RateSampler.sample?(token)

      assert :ets.lookup(:source_rate_sampler, token) == before
    end
  end

  describe "bump/2" do
    test "a large first batch on a never-seen source seeds a provisional rate immediately" do
      token = unique_token()

      RateSampler.bump(token, 5_000)

      # 5,000 events landing within (at most) the current window projects to
      # a rate far above 1/sec -- sampling should already be well below 1.0
      # for this very first batch, not just for the next one.
      sampled = for _ <- 1..2_000, do: RateSampler.sample?(token)
      sampled_count = Enum.count(sampled, & &1)

      assert sampled_count < 100,
             "expected a large first burst to be throttled immediately, got #{sampled_count}/2000"
    end

    test "a small first batch samples close to everything" do
      token = unique_token()

      RateSampler.bump(token, 1)

      assert RateSampler.sample?(token)
    end

    test "bumping the same window multiple times accumulates the count" do
      token = unique_token()

      RateSampler.bump(token, 10)
      RateSampler.bump(token, 10)

      assert [{^token, _window_start, 20, _rate}] = :ets.lookup(:source_rate_sampler, token)
    end
  end
end
