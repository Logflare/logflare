defmodule Logflare.MemoryRepo do
  use Ecto.Repo,
    otp_app: :logflare,
    adapter: Ecto.Adapters.Mnesia
end
