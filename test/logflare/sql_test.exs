defmodule Logflare.SqlTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.SQL
  alias Logflare.SqlV2
  alias  Ecto.Adapters.SQL.Sandbox
  @project_id "logflare-dev-238720"
  @env "test"
  setup do
    start_supervised!(SQL)


    on_exit(fn->
      Sandbox.unboxed_run(Logflare.Repo, fn->
        Logflare.Repo.delete_all(Logflare.User)
        Logflare.Repo.delete_all(Logflare.Source)
      end)
    end)
    :ok
  end
  test "transform table names" do
    Sandbox.unboxed_run(Logflare.Repo, fn->
      user = insert(:user)
      source = insert(:source, user: user, name: "my_table") |> IO.inspect()
      query = "select val from my_table where my_table.val > 5"
      assert SQL.transform(query, user)  == SqlV2.transform(query, user)
      assert SqlV2.transform(query, user) == "select val from #{bq_table_name(user)} where #{bq_table_name(user)}.val > 5"
    end)
  end


  test "transform table names backquoted" do
    Sandbox.unboxed_run(Logflare.Repo, fn->
      user = insert(:user)
      source = insert(:source, user: user, name: "my_table") |> IO.inspect()
      query = "select val from my_table where my_table.val > 5"
      assert SQL.transform(query, user)  == SqlV2.transform(query, user)
      assert SqlV2.transform(query, user) == "select val from `#{bq_table_name(user)}` where `#{bq_table_name(user)}`.val > 5"
    end)
  end


#   private fun tableName(sourceName: String): String  {
#     val source = sourceResolver().resolve(sourceName)
#     return "`${projectId}.${datasetResolver().resolve(source)}.${DefaultTableResolver.resolve(source)}`"
# }
  defp bq_table_name(%{user: user} = source) do
     "#{@project_id}.#{user.id}_#{@env}.#{inspect(source.token)}"
  end


end
