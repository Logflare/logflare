defmodule Logflare.Utils do
  @moduledoc """
  Context-only utilities. Should not be used outside of `lib/logflare/*`
  """
  import Cachex.Spec

  @doc """
  Builds a long Cachex expiration spec
  Defaults to 20 min with 5 min cleanup intervals
  """
  @spec cache_expiration_min(non_neg_integer(), non_neg_integer()) :: Cachex.Spec.expiration()
  def cache_expiration_min(default \\ 20, interval \\ 5) do
    cache_expiration_sec(default * 60, interval * 60)
  end

  @doc """
  Builds a short Cachex expiration spec
  Defaults to 50 sec with 20 sec cleanup intervals
  """
  @spec cache_expiration_sec(non_neg_integer(), non_neg_integer()) :: Cachex.Spec.expiration()
  def cache_expiration_sec(default \\ 60, interval \\ 20) do
    expiration(
      # default record expiration of 20 mins
      default: :timer.seconds(default),
      # how often cleanup should occur, 5 mins
      interval: :timer.seconds(interval),
      # whether to enable lazy checking
      lazy: true
    )
  end
end
