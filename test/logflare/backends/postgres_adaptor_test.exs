defmodule Logflare.Backends.Adaptor.PostgresAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.PostgresAdaptor

  import Ecto.Query

  setup do
    repo = Application.get_env(:logflare, Logflare.Repo)

    url =
      "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"

    config = %{
      "url" => url
    }

    source = insert(:source, user: insert(:user))
    source_backend = insert(:source_backend, type: :postgres, source: source, config: config)
    pid = start_supervised!({PostgresAdaptor, source_backend})

    on_exit(fn ->
      PostgresAdaptor.rollback_migrations(source_backend)
      PostgresAdaptor.drop_migrations_table(source_backend)
    end)

    %{pid: pid, source_backend: source_backend}
  end

  test "ingest/2 and execute_query/2 dispatched message", %{
    pid: pid,
    source_backend: source_backend
  } do
    log_event =
      build(:log_event,
        source: source_backend.source,
        test: "data"
      )

    assert :ok = PostgresAdaptor.ingest(pid, [log_event])

    # TODO: replace with a timeout retry func
    :timer.sleep(1_500)

    query =
      from(l in PostgresAdaptor.Repo.table_name(source_backend),
        select: l.body
      )

    assert [
             %{
               "test" => "data"
             }
           ] = PostgresAdaptor.execute_query(pid, query)
  end
end
