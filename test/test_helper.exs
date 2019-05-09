ExUnit.start()

Mox.defmock(Logflare.Users.APIMock, for: Logflare.Users.API)
Mox.defmock(Logflare.TableCounterMock, for: Logflare.TableCounter)

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :manual)
