defmodule Logflare.Google.BigQuery.GenUtils do
  @doc """
  Generic utils for BigQuery.
  """
  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.User

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @table_ttl 604_800_000

  @spec get_project_id(:atom) :: String.t()
  def get_project_id(source) do
    %Logflare.Source{user_id: user_id} = Repo.get_by(Source, token: Atom.to_string(source))
    %Logflare.User{bigquery_project_id: project_id} = Repo.get(User, user_id)

    if is_nil(project_id) do
      @project_id
    else
      project_id
    end
  end

  @spec get_table_ttl(:atom) :: String.t()
  def get_table_ttl(source) do
    %Logflare.Source{user_id: user_id, bigquery_table_ttl: ttl} =
      Repo.get_by(Source, token: Atom.to_string(source))

    %Logflare.User{bigquery_project_id: project_id} = Repo.get(User, user_id)

    cond do
      is_nil(project_id) -> @table_ttl
      is_nil(ttl) -> @table_ttl
      true -> ttl * 86_400_000
    end
  end
end
