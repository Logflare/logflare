defmodule Logflare.Repo.Migrations.NilifyConfigColumnForBackendsTable do
  use Ecto.Migration
  import Ecto.Query
  alias Logflare.Ecto.EncryptedMap

  def up do
    from(b in "backends", update: [set: [config: nil]])
    |> Logflare.Repo.update_all([])
  end

  def down do
    {:ok, pid} = Logflare.Vault.start_link()

    # copy configs over
    Logflare.Repo.all(from b in "backends", select: [:id, :config_encrypted])
    |> Enum.each(fn %{id: id} = backend ->
      {:ok, config} = EncryptedMap.load(backend.config_encrypted)

      from(b in "backends",
        where: b.id == ^id,
        update: [set: [config: ^config]]
      )
      |> Logflare.Repo.update_all([])
    end)
    # stop the vault
    Process.unlink(pid)
    Process.exit(pid, :kill)
    :timer.sleep(100)
  end
end
