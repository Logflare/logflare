defmodule Logflare.Google.BigQuery.GenUtils do
  @doc """
  Generic utils for BigQuery.
  """
  alias Logflare.{Sources, Users}
  alias GoogleApi.BigQuery.V2.Connection

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @table_ttl 604_800_000

  @spec get_project_id(atom()) :: String.t()
  def get_project_id(source_id) when is_atom(source_id) do
    %Logflare.Source{user_id: user_id} = Sources.Cache.get_by_id(source_id)
    %Logflare.User{bigquery_project_id: project_id} = Users.Cache.get_by_id(user_id)

    if is_nil(project_id) do
      @project_id
    else
      project_id
    end
  end

  @spec get_table_ttl(atom()) :: non_neg_integer()
  def get_table_ttl(source_id) when is_atom(source_id) do
    %Logflare.Source{user_id: user_id, bigquery_table_ttl: ttl} =
      Sources.Cache.get_by_id(source_id)

    %Logflare.User{bigquery_project_id: project_id} = Users.Cache.get_by_id(user_id)

    cond do
      is_nil(project_id) -> @table_ttl
      is_nil(ttl) -> @table_ttl
      true -> ttl * 86_400_000
    end
  end

  @spec format_table_name(atom) :: String.t()
  def format_table_name(source) do
    string = Atom.to_string(source)
    String.replace(string, "-", "_")
  end

  def get_conn() do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    Connection.new(token.token)
  end

  @spec get_account_id(atom) :: String.t()
  def get_account_id(source_id) do
    %Logflare.Source{user_id: account_id} = Sources.Cache.get_by_id(source_id)
    "#{account_id}"
  end

  @spec get_tesla_error_message(%Tesla.Env{}) :: String.t()
  def get_tesla_error_message(message) do
    {:ok, message_body} = Jason.decode(message.body)
    message_body["error"]["message"]
  end
end
