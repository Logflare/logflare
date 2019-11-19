defmodule Logflare.Google.BigQuery.GenUtils do
  @moduledoc """
  Generic utils for BigQuery.
  """
  alias Logflare.{Sources, Users}
  alias GoogleApi.BigQuery.V2.Connection

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @table_ttl 604_800_000
  @default_dataset_location "US"
  @default_table_name_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append] ||
                               ""

  @spec get_project_id(atom()) :: String.t()
  def get_project_id(source_id) when is_atom(source_id) do
    %Logflare.Source{user_id: user_id} = Sources.Cache.get_by_id(source_id)
    %Logflare.User{bigquery_project_id: project_id} = Users.Cache.get_by(id: user_id)

    project_id || @project_id
  end

  def get_bq_user_info(source_id) when is_atom(source_id) do
    %Logflare.User{
      id: user_id,
      sources: sources,
      bigquery_project_id: project_id,
      bigquery_dataset_location: dataset_location,
      bigquery_dataset_id: dataset_id
    } = Users.get_by_source(source_id)

    %Logflare.Source{bigquery_table_ttl: ttl} =
      Enum.find(sources, fn x -> x.token == source_id end)

    new_ttl =
      cond do
        is_nil(project_id) -> @table_ttl
        is_nil(ttl) -> @table_ttl
        true -> ttl * 86_400_000
      end

    new_project_id = project_id || @project_id
    new_dataset_location = dataset_location || @default_dataset_location
    new_dataset_id = dataset_id || "#{user_id}" <> @default_table_name_append

    %{
      user_id: user_id,
      bigquery_table_ttl: new_ttl,
      bigquery_project_id: new_project_id,
      bigquery_dataset_location: new_dataset_location,
      bigquery_dataset_id: new_dataset_id
    }
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
  def get_account_id(source_id) when is_atom(source_id) do
    %Logflare.Source{user_id: account_id} = Sources.Cache.get_by_id(source_id)
    "#{account_id}"
  end

  @spec get_tesla_error_message(:emfile | :timeout | Tesla.Env.t()) :: any
  def get_tesla_error_message(%Tesla.Env{} = message) do
    case Jason.decode(message.body) do
      {:ok, message_body} ->
        message_body["error"]["message"]

      {:error, message} ->
        "#{message}"
    end
  end

  def get_tesla_error_message(:emfile), do: "emfile"
  def get_tesla_error_message(:timeout), do: "timeout"
  def get_tesla_error_message(:closed), do: "closed"
  def get_tesla_error_message(message), do: "#{message}"
end
