ExUnit.start()

Mox.defmock(Logflare.Users.APIMock, for: Logflare.Users.API)

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :manual)
